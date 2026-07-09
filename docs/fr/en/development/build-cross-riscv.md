---
description: 'Guide pour compiler ClickHouse depuis les sources pour l’architecture RISC-V 64'
sidebar_label: 'Compiler sous Linux pour RISC-V 64'
sidebar_position: 30
slug: /development/build-cross-riscv
title: 'Comment compiler ClickHouse sous Linux pour RISC-V 64'
doc_type: 'guide'
---

La prise en charge de RISC-V par ClickHouse est expérimentale. Il n’est pas possible d’activer toutes les fonctionnalités.

<div id="build-clickhouse">
  ## Compiler ClickHouse
</div>

Pour effectuer une compilation croisée pour RISC-V depuis une machine non RISC-V :

```bash
cd ClickHouse
mkdir build-riscv64
cmake . -Bbuild-riscv64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-riscv64.cmake -DGLIBC_COMPATIBILITY=OFF -DENABLE_LDAP=OFF  -DOPENSSL_NO_ASM=ON -DENABLE_JEMALLOC=ON -DENABLE_PARQUET=OFF -DENABLE_GRPC=OFF -DENABLE_HDFS=OFF -DENABLE_MYSQL=OFF
ninja -C build-riscv64
```

Le binaire résultant ne fonctionnera que sous Linux, sur une architecture CPU RISC-V 64.