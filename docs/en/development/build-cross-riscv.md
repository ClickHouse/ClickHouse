---
sidebar_position: 68
sidebar_label: Build on Linux for RISC-V 64
---

# How to Build ClickHouse on Linux for RISC-V 64 Architecture 

As of writing (11.11.2021) building for risc-v considered to be highly experimental. Not all features can be enabled.

This is for the case when you have Linux machine and want to use it to build `clickhouse` binary that will run on another Linux machine with RISC-V 64 CPU architecture. This is intended for continuous integration checks that run on Linux servers.

The cross-build for RISC-V 64 is based on the [Build instructions](../development/build.md), follow them first.

## Install Clang-14

Follow the instructions from https://apt.llvm.org/ for your Ubuntu or Debian setup or do
```
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

## Build ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-riscv64
CC=clang-14 CXX=clang++-14 cmake . -Bbuild-riscv64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-riscv64.cmake -DGLIBC_COMPATIBILITY=OFF -DENABLE_LDAP=OFF  -DOPENSSL_NO_ASM=ON -DENABLE_JEMALLOC=ON -DENABLE_PARQUET=OFF -DUSE_UNWIND=OFF -DENABLE_GRPC=OFF -DENABLE_HDFS=OFF -DENABLE_MYSQL=OFF
ninja -C build-riscv64
```

The resulting binary will run only on Linux with the RISC-V 64 CPU architecture.
