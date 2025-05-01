---
description: 'Guide for building ClickHouse from source for the LoongArch64 architecture'
sidebar_label: 'Build on Linux for LoongArch64'
sidebar_position: 35
slug: /development/build-cross-loongarch
title: 'Build on Linux for LoongArch64'
---

# Build on Linux for LoongArch64

ClickHouse has experimental support for LoongArch64

## Build ClickHouse {#build-clickhouse}

The llvm version required for building must be greater than or equal to 19.1.0.

```bash
cd ClickHouse
mkdir build-loongarch64
CC=clang-19 CXX=clang++-19 cmake . -Bbuild-loongarch64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-loongarch64.cmake
ninja -C build-loongarch64
```

The resulting binary will run only on Linux with the LoongArch64 CPU architecture.
