#[macro_use]
extern crate log;

use futures;
use futures::future::{ok as FutOk, Future};
use futures::lazy;
use futures::{IntoFuture, Stream};
use lapin_futures::channel::{
    BasicConsumeOptions, BasicProperties, BasicPublishOptions, Channel, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin_futures::client::{Client, ConnectionOptions};
use lapin_futures::consumer::Consumer;
use lapin_futures::types::FieldTable;
use serde_json;
use std::io;
use std::net::SocketAddr;
use std::str;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::thread;
use std::thread::JoinHandle;
use tokio;
use tokio::net::TcpStream;
use tokio::runtime::current_thread::block_on_all;
use tokio::runtime::Runtime;

struct Store {
    event_stream: Stream<Item = (), Error = ()>,
}

fn connect(
    uri: SocketAddr,
    exchange: String,
) -> impl Future<Item = (Client<TcpStream>, Channel<TcpStream>), Error = ()> {
    let exchange1 = exchange.clone();

    TcpStream::connect(&uri)
        .and_then(|stream| Client::connect(stream, ConnectionOptions::default()))
        .and_then(|(client, heartbeat)| {
            trace!("Start heartbeat");

            tokio::spawn(heartbeat.map_err(|e| eprintln!("heartbeat error: {:?}", e)))
                .into_future()
                .map(|_| client)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "spawn error"))
        })
        .and_then(move |client| {
            trace!("Set up channel");

            client
                .create_channel()
                .map(move |channel| (client, channel))
        })
        .and_then(move |(client, channel)| {
            trace!("Exchange declare");

            channel
                .exchange_declare(
                    &exchange1,
                    &"topic",
                    ExchangeDeclareOptions {
                        durable: true,
                        ..ExchangeDeclareOptions::default()
                    },
                    FieldTable::new(),
                )
                .map(|_| (client, channel))
        })
        .map_err(|e| panic!("Shiet {:?}", e))
}

fn create_consumer(
    channel: Channel<TcpStream>,
    queue_name: String,
) -> impl Future<Item = (Channel<TcpStream>, Consumer<TcpStream>), Error = ()> + Send + 'static {
    // let event_name = "foo.Bar".to_string();

    // let _channel = channel.clone();
    // let _queue = queue_name.clone();

    info!("will create consumer for {}", queue_name);

    channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions {
                durable: true,
                exclusive: false,
                auto_delete: false,
                ..QueueDeclareOptions::default()
            },
            FieldTable::new(),
        )
        // .and_then(|_| {
        //     trace!("ONE");
        //     FutOk(())
        // })
        .map(|queue| (channel, queue))
        .and_then(move |(channel, queue)| {
            trace!("TWO");
            channel
                .queue_bind(
                    &queue_name,
                    // TODO: Pass in
                    "iris",
                    &queue_name,
                    QueueBindOptions::default(),
                    FieldTable::new(),
                )
                .map(|_| (channel, queue))
        })
        .and_then(move |(channel, queue)| {
            info!("creating consumer {}", 0);
            channel
                .basic_consume(
                    &queue,
                    // TODO: Pass in
                    "iris",
                    BasicConsumeOptions::default(),
                    FieldTable::new(),
                )
                .map(move |stream| (channel, stream))
        })
        .map_err(|e| panic!("Hmm {:?}", e))
    // .and_then(move |(channel, stream)| {
    //     info!("got stream for consumer {}", 0);
    //     // stream.for_each(move |message| {
    //     //     let payload = str::from_utf8(&message.data).unwrap();
    //     //     let data: Event<E> = serde_json::from_str(payload).unwrap();
    //     //     trace!("Received message with ID {}: {}", data.id, payload);
    //     //     handler(&data);
    //     //     channel.basic_ack(message.delivery_tag, false)
    //     // })
    //     stream
    // })
    // .map(|_| ())
    // .map_err(move |err| error!("got error in consumer {:?}", err))
}

// fn main() {
//     pretty_env_logger::init();

//     let uri: SocketAddr = "0.0.0.0:5672"
//         .parse()
//         .expect("Could not parse AMQP endpoint address");

//     info!("Binding to {}", uri);

//     let fut = connect(uri, "iris".to_string())
//         .and_then(|(_client, channel)| create_consumer(channel, "queue.Name".to_string()))
//         .and_then(|(channel, stream)| {
//             stream
//                 .for_each(move |message| {
//                     println!("Event!");

//                     channel.basic_ack(message.delivery_tag, false)
//                 })
//                 .map_err(|e| {
//                     error!("{:?}", e);
//                     ()
//                 })
//         });

//     let handle = thread::spawn(|| {
//         block_on_all(fut);
//     });

//     trace!("End program");
// }

struct Thingy {
    tx: Sender<i32>,
    rx: Receiver<i32>,
    tx_handles: Vec<JoinHandle<()>>,
}

impl Thingy {
    pub fn new() -> Self {
        let (tx, rx) = channel();

        Self {
            tx,
            rx,
            tx_handles: Vec::new(),
        }
    }

    pub fn register_listener(&mut self, queue_name: String) {
        let uri: SocketAddr = "0.0.0.0:5672"
            .parse()
            .expect("Could not parse AMQP endpoint address");

        info!("Binding to {}", uri);

        let tx = self.tx.clone();

        let fut = connect(uri, "iris".to_string())
            .and_then(|(_client, channel)| create_consumer(channel, queue_name))
            .and_then(|(channel, stream)| {
                stream
                    .for_each(move |message| {
                        println!("Event!");

                        tx.send(1i32).expect("Failed to send");

                        channel.basic_ack(message.delivery_tag, false)
                    })
                    .map_err(|e| {
                        error!("{:?}", e);

                        ()
                    })
            });

        trace!("Before sender spawn");

        let handle = thread::spawn(move || {
            block_on_all(fut).expect("Block failed");
        });

        self.tx_handles.push(handle);
    }

    pub fn run(self) -> JoinHandle<()> {
        trace!("Running {} handles", self.tx_handles.len());

        let rx = self.rx;

        let join_handle = thread::spawn(move || {
            while let Ok(n) = rx.recv() {
                println!("Received {}", n);
            }
        });

        join_handle
    }
}

fn main() {
    pretty_env_logger::init();

    // Create a channel with a sending end (tx) and a receiving end (rx).
    // let (tx, rx) = channel();

    // // Spawn a new thread, and move the receiving end into the thread.
    // let join_handle = thread::spawn(move || {
    //     // Keep receiving in a loop, until tx is dropped!
    //     while let Ok(n) = rx.recv() {
    //         // Note: `recv()` always blocks
    //         println!("Received {}", n);
    //     }
    // });

    // let uri: SocketAddr = "0.0.0.0:5672"
    //     .parse()
    //     .expect("Could not parse AMQP endpoint address");

    // info!("Binding to {}", uri);

    // let fut = connect(uri, "iris".to_string())
    //     .and_then(|(_client, channel)| create_consumer(channel, "queue.Name".to_string()))
    //     .and_then(|(channel, stream)| {
    //         stream
    //             .for_each(move |message| {
    //                 println!("Event!");

    //                 tx.send(1i32).expect("Failed to send");

    //                 channel.basic_ack(message.delivery_tag, false)
    //             })
    //             .map_err(|e| {
    //                 error!("{:?}", e);
    //                 ()
    //             })
    //     });

    // trace!("Before sender spawn");

    // let handle = thread::spawn(move || {
    //     block_on_all(fut).expect("Block failed");
    // });

    // trace!("After sender spawn");

    // // for i in 0..10 {
    // //     tx.send(i).unwrap(); // Note: `send()` never blocks
    // // }

    // // // Drop `tx` so that `rx.recv()` returns an `Err(_)`.
    // // drop(tx);

    // // // Wait for the spawned thread to finish.
    // handle.join().unwrap();

    let mut thing = Thingy::new();

    thing.register_listener("queue.Name".into());
    thing.register_listener("queue.OtherName".into());

    let handle = thing.run();

    handle.join().unwrap();
}
