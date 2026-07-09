---
description: 'AARCH64 架构下从源码构建 ClickHouse 指南'
sidebar_label: '在 Linux 上为 AARCH64 构建'
sidebar_position: 25
slug: /development/build-cross-arm
title: '如何在 Linux 上为 AARCH64 构建 ClickHouse'
doc_type: 'guide'
---

在 Aarch64 机器上为 Aarch64 构建 ClickHouse 无需任何特殊步骤。

如需在 x86 Linux 机器上为 AArch64 交叉编译 ClickHouse，请向 `cmake` 传递以下参数：`-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`