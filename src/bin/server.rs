use bytes::Bytes;
use mini_redis::{Connection, Frame};
// use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
// use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

// const MAX_SHARDS: usize = 10;
type Db = Arc<Mutex<HashMap<String, Bytes>>>;
// type ShardedDb = Arc<Vec<Mutex<HashMap<String, Bytes>>>>;

// fn new_sharded_db(num_shards: usize) -> ShardedDb {
//     let mut db = Vec::with_capacity(num_shards);
//     for _ in 0..num_shards {
//         db.push(Mutex::new(HashMap::new()));
//     }
//     Arc::new(db)
// }

// fn calculate_hash<T: Hash>(t: &T) -> u64 {
//     let mut s = DefaultHasher::new();
//     t.hash(&mut s);
//     s.finish()
// }

// fn calculate_shard_index(t:String, db_len: usize)->usize{
//     let mut ret: u64 = calculate_hash(&t);
//     ret =  ret % db_len.into();
//     ret.try_into().unwrap()
// }

/// A TCP server listens for connections on a port by keeping track of a bunch of sockets.
/// When  a socket is flagged, a client has arrived for connection, then we move onto handshaking with them
/// Setps in setting up a tcp server...
/// 1. Server IP:PORT to listen to
/// 2. Bind Server IP:PORT for listening
/// 3. Check for incoming connections on sockets
/// 4. accept connection and process it
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("Listening");

    // let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let db = Db::default();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        // Clone the handle to the hash map.
        let db = db.clone();

        println!("Accepted");
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}
async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};

    // Connection, provided by `mini-redis`, handles parsing frames from
    // the socket
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }
}
