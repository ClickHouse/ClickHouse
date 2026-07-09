---
description: 'Guía para compilar ClickHouse a partir del código fuente para la arquitectura RISC-V 64'
sidebar_label: 'Compilar en Linux para RISC-V 64'
sidebar_position: 30
slug: /development/build-cross-riscv
title: 'Cómo compilar ClickHouse en Linux para RISC-V 64'
doc_type: 'guide'
---

ClickHouse ofrece compatibilidad experimental con RISC-V. No se pueden habilitar todas las funcionalidades.

<div id="build-clickhouse">
  ## Compilar ClickHouse
</div>

Para compilar de forma cruzada para RISC-V desde una máquina que no sea RISC-V:

```bash
cd ClickHouse
mkdir build-riscv64
cmake . -Bbuild-riscv64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-riscv64.cmake -DGLIBC_COMPATIBILITY=OFF -DENABLE_LDAP=OFF  -DOPENSSL_NO_ASM=ON -DENABLE_JEMALLOC=ON -DENABLE_PARQUET=OFF -DENABLE_GRPC=OFF -DENABLE_HDFS=OFF -DENABLE_MYSQL=OFF
ninja -C build-riscv64
```

El binario resultante solo se ejecutará en Linux con la arquitectura de CPU RISC-V de 64 bits.