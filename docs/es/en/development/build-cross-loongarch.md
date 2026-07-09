---
description: 'Guía para compilar ClickHouse desde el código fuente para la arquitectura LoongArch64'
sidebar_label: 'Compilar en Linux para LoongArch64'
sidebar_position: 35
slug: /development/build-cross-loongarch
title: 'Compilar en Linux para LoongArch64'
doc_type: 'guide'
---

ClickHouse ofrece soporte experimental para LoongArch64

<div id="build-clickhouse">
  ## Compilar ClickHouse
</div>

La versión de LLVM necesaria para compilar debe ser la 21.1.0 o una superior.

```bash
cd ClickHouse
mkdir build-loongarch64
cmake . -Bbuild-loongarch64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-loongarch64.cmake
ninja -C build-loongarch64
```

El binario resultante solo se ejecutará en Linux con CPU de arquitectura LoongArch64.