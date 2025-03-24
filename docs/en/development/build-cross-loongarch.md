---
slug: /en/development/build-cross-loongarch
sidebar_position: 70
title: How to Build ClickHouse on Linux for LoongArch64 Architecture 
sidebar_label: Build on Linux for LoongArch64
---

As of writing (2024/03/15) building for loongarch considered to be highly experimental. Not all features can be enabled.

This is for the case when you have Linux machine and want to use it to build `clickhouse` binary that will run on another Linux machine with LoongArch64 CPU architecture. This is intended for continuous integration checks that run on Linux servers.

The cross-build for LoongArch64 is based on the [Build instructions](../development/build.md), follow them first.

## Install Clang-18

Follow the instructions from https://apt.llvm.org/ for your Ubuntu or Debian setup or do
```
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

## Build ClickHouse {#build-clickhouse}


The llvm version required for building must be greater than or equal to 18.1.0.
``` bash
cd ClickHouse
mkdir build-loongarch64
CC=clang-18 CXX=clang++-18 cmake . -Bbuild-loongarch64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-loongarch64.cmake
ninja -C build-loongarch64
```

The resulting binary will run only on Linux with the LoongArch64 CPU architecture.
