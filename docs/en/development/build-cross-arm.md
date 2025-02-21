---
slug: /development/build-cross-arm
sidebar_position: 25
sidebar_label: Build on Linux for AARCH64
---

# How to Build ClickHouse on Linux for AARCH64

No special steps are required to build ClickHouse for Aarch64 on an Aarch64 machine.

To cross compile ClickHouse for AArch64 on an x86 Linux machine, pass the following flag to `cmake`: `-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`
