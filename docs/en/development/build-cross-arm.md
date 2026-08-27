---
description: 'Guide for building ClickHouse from source for the AARCH64 architecture'
sidebar_label: 'Build on Linux for AARCH64'
sidebar_position: 25
slug: /development/build-cross-arm
title: 'How to Build ClickHouse on Linux for AARCH64'
---

# How to Build ClickHouse on Linux for AARCH64

No special steps are required to build ClickHouse for Aarch64 on an Aarch64 machine.

To cross compile ClickHouse for AArch64 on an x86 Linux machine, pass the following flag to `cmake`: `-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`
