# How to use Address Sanitizer

Note: We use Address Sanitizer to run functional tests for every commit automatically.

```
mkdir build && cd build
```

Note:
ENABLE_TCMALLOC=0 is optional.
CC=clang CXX=clang++ is strongly recommended.

```
CC=clang CXX=clang++ cmake -D CMAKE_BUILD_TYPE=ASan -D ENABLE_TCMALLOC=0 ..
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
mkdir build && cd build
```

## Note: All parameters are mandatory.

```
CC=clang CXX=clang++ cmake -D CMAKE_BUILD_TYPE=TSan -D ENABLE_TCMALLOC=0 ..
ninja
```

## Copy binary to your server

```
scp ./dbms/programs/clickhouse yourserver:~/clickhouse-tsan
```

## Start ClickHouse and run tests

```
sudo -u clickhouse TSAN_OPTIONS='halt_on_error=1' ./clickhouse-tsan server --config /etc/clickhouse-server/config.xml
```


# How to use Memory Sanitizer

First, build libc++ with MSan:

```
svn co http://llvm.org/svn/llvm-project/llvm/trunk llvm
(cd llvm/projects && svn co http://llvm.org/svn/llvm-project/libcxx/trunk libcxx)
(cd llvm/projects && svn co http://llvm.org/svn/llvm-project/libcxxabi/trunk libcxxabi)

mkdir libcxx_msan && cd libcxx_msan
cmake ../llvm -DCMAKE_BUILD_TYPE=Release -DLLVM_USE_SANITIZER=Memory -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
make cxx -j24
```

Then, build ClickHouse:

```
mkdir build && cd build
```

```
CC=clang CXX=clang++ cmake -D CMAKE_BUILD_TYPE=MSan -D LIBCXX_PATH=/home/milovidov/libcxx_msan ..
```
