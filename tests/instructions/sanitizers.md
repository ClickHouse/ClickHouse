# How to use Address Sanitizer

Note: We use Address Sanitizer to run functional tests for every commit automatically.

```
mkdir build_asan && cd build_asan
```

Note: using clang instead of gcc is strongly recommended. Make sure you have installed required packages (`clang`, `lld`). It may be required to specify non-standard `lld` binary using `LINKER_NAME` option (e.g. `-D LINKER_NAME=lld-8`).

```
CC=clang CXX=clang++ cmake -D SANITIZE=address ..
ninja
```

## Copy binary to your server

```
scp ./programs/clickhouse yourserver:~/clickhouse-asan
```

## Start ClickHouse and run tests

```
sudo -u clickhouse ./clickhouse-asan server --config /etc/clickhouse-server/config.xml
```


# How to use Thread Sanitizer

```
mkdir build_tsan && cd build_tsan
```

```
CC=clang CXX=clang++ cmake -D SANITIZE=thread ..
ninja
```

## Start ClickHouse and run tests

```
sudo -u clickhouse TSAN_OPTIONS='halt_on_error=1' ./clickhouse-tsan server --config /etc/clickhouse-server/config.xml
```


# How to use Undefined Behaviour Sanitizer

```
mkdir build_ubsan && cd build_ubsan
```

Note: clang is mandatory, because gcc (in version 8) has false positives due to devirtualization and it has less amount of checks.

```
CC=clang CXX=clang++ cmake -D SANITIZE=undefined ..
ninja
```

## Start ClickHouse and run tests

```
sudo -u clickhouse UBSAN_OPTIONS='print_stacktrace=1' ./clickhouse-ubsan server --config /etc/clickhouse-server/config.xml
```


# How to use Memory Sanitizer

```
CC=clang-8 CXX=clang++-8 cmake -D ENABLE_HDFS=0 -D ENABLE_CAPNP=0 -D ENABLE_RDKAFKA=0 -D ENABLE_ICU=0 -D ENABLE_POCO_MONGODB=0 -D ENABLE_POCO_NETSSL=0 -D ENABLE_ODBC=0 -D ENABLE_MYSQL=0 -D ENABLE_EMBEDDED_COMPILER=0 -D USE_SIMDJSON=0 -D SANITIZE=memory ..
```
