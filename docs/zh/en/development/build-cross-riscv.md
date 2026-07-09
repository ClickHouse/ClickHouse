---
description: 'RISC-V 64 架构的 ClickHouse 从源码构建指南'
sidebar_label: '在 Linux 上为 RISC-V 64 构建'
sidebar_position: 30
slug: /development/build-cross-riscv
title: '如何在 Linux 上为 RISC-V 64 构建 ClickHouse'
doc_type: 'guide'
---

ClickHouse 对 RISC-V 提供 Experimental 支持，并非所有功能都能启用。

<div id="build-clickhouse">
  ## 构建 ClickHouse
</div>

如需在非 RISC-V 机器上为 RISC-V 进行交叉编译：

```bash
cd ClickHouse
mkdir build-riscv64
cmake . -Bbuild-riscv64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-riscv64.cmake -DGLIBC_COMPATIBILITY=OFF -DENABLE_LDAP=OFF  -DOPENSSL_NO_ASM=ON -DENABLE_JEMALLOC=ON -DENABLE_PARQUET=OFF -DENABLE_GRPC=OFF -DENABLE_HDFS=OFF -DENABLE_MYSQL=OFF
ninja -C build-riscv64
```

生成的二进制文件只能在 RISC-V 64 CPU 架构的 Linux 系统上运行。