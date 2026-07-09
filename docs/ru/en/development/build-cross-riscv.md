---
description: 'Руководство по сборке ClickHouse из исходного кода для архитектуры RISC-V 64'
sidebar_label: 'Сборка на Linux для RISC-V 64'
sidebar_position: 30
slug: /development/build-cross-riscv
title: 'Как собрать ClickHouse на Linux для RISC-V 64'
doc_type: 'guide'
---

ClickHouse имеет экспериментальную поддержку RISC-V. Не все возможности можно включить.

<div id="build-clickhouse">
  ## Сборка ClickHouse
</div>

Чтобы выполнить кросс-компиляцию для RISC-V на машине с другой архитектурой:

```bash
cd ClickHouse
mkdir build-riscv64
cmake . -Bbuild-riscv64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-riscv64.cmake -DGLIBC_COMPATIBILITY=OFF -DENABLE_LDAP=OFF  -DOPENSSL_NO_ASM=ON -DENABLE_JEMALLOC=ON -DENABLE_PARQUET=OFF -DENABLE_GRPC=OFF -DENABLE_HDFS=OFF -DENABLE_MYSQL=OFF
ninja -C build-riscv64
```

Полученный бинарный файл будет работать только в Linux на архитектуре RISC-V 64.