# How to use Address Sanitizer

Note: We use Address Sanitizer to run functional tests for every commit automatically.

```
mkdir build_asan && cd build_asan
```

Note: using clang instead of gcc is strongly recommended.

```
CC=clang CXX=clang++ cmake -D SANITIZE=address ..
ninja
```

## Copy binary to your server

```
scp ./dbms/programs/clickhouse yourserver:~/clickhouse-asan
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
sudo -u clickhouse TSAN_OPTIONS='halt_on_error=1,suppressions=../dbms/tests/tsan_suppressions.txt' ./clickhouse-tsan server --config /etc/clickhouse-server/config.xml
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

TODO
