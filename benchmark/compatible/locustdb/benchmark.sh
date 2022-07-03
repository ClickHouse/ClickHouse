#!/bin/bash

# https://rustup.rs/
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

sudo apt-get update
sudo apt-get install -y git

git clone https://github.com/cswinter/LocustDB.git
cd LocustDB

sudo apt-get install -y g++ capnproto libclang-14-dev

cargo build --features "enable_rocksdb" --features "enable_lz4" --release

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

target/release/repl --load hits.csv --db-path db

# Loaded data in 920s.
# Table `default` (99997496 rows, 15.0GiB)

# SELECT * FROM default LIMIT 1

# And it immediately panicked and hung:

#locustdb> SELECT * FROM default LIMIT 1
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
#thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
