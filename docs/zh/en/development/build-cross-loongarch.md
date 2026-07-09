---
description: '面向 LoongArch64 架构的 ClickHouse 从源码构建指南'
sidebar_label: '在 Linux 上为 LoongArch64 构建'
sidebar_position: 35
slug: /development/build-cross-loongarch
title: '在 Linux 上为 LoongArch64 构建'
doc_type: 'guide'
---

ClickHouse 已对 LoongArch64 提供实验性支持

<div id="build-clickhouse">
  ## 构建 ClickHouse
</div>

构建时所需的 llvm 版本必须大于或等于 21.1.0。

```bash
cd ClickHouse
mkdir build-loongarch64
cmake . -Bbuild-loongarch64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-loongarch64.cmake
ninja -C build-loongarch64
```

生成的二进制文件只能在采用 LoongArch64 CPU 架构的 Linux 系统上运行。