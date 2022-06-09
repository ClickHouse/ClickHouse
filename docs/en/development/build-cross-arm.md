---
sidebar_position: 67
sidebar_label: Build on Linux for AARCH64 (ARM64)
---

# How to Build ClickHouse on Linux for AARCH64 (ARM64) Architecture 

This is for the case when you have Linux machine and want to use it to build `clickhouse` binary that will run on another Linux machine with AARCH64 CPU architecture. 
This is intended for continuous integration checks that run on Linux servers.

The cross-build for AARCH64 is based on the [Build instructions](../development/build.md), follow them first.

## Install Clang-14 or newer

Follow the instructions from https://apt.llvm.org/ for your Ubuntu or Debian setup or do
```
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

## Install Cross-Compilation Toolset {#install-cross-compilation-toolset}

``` bash
cd ClickHouse
mkdir -p build-aarch64/cmake/toolchain/linux-aarch64
wget 'https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en' -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
tar xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz -C build-aarch64/cmake/toolchain/linux-aarch64 --strip-components=1
```

## Build ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-arm64
CC=clang-14 CXX=clang++-14 cmake . -Bbuild-arm64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake
ninja -C build-arm64
```

The resulting binary will run only on Linux with the AARCH64 CPU architecture.
