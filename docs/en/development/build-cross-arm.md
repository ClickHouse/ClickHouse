---
sidebar_position: 67
sidebar_label: Build on Linux for AARCH64 (ARM64)
---

# How to Build ClickHouse on Linux for AARCH64 (ARM64) Architecture 

If you use AArch64 machine and want to build ClickHouse for AArch64, build as usual.

If you use x86_64 machine and want cross-compile for AArch64, add the following flag to `cmake`: `-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`
