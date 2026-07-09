---
description: 'Guia para compilar o ClickHouse a partir do código-fonte para a arquitetura LoongArch64'
sidebar_label: 'Compilar no Linux para LoongArch64'
sidebar_position: 35
slug: /development/build-cross-loongarch
title: 'Compilar no Linux para LoongArch64'
doc_type: 'guide'
---

O ClickHouse oferece suporte experimental ao LoongArch64

<div id="build-clickhouse">
  ## Compilar o ClickHouse
</div>

A versão do LLVM necessária para a compilação deve ser a 21.1.0 ou superior.

```bash
cd ClickHouse
mkdir build-loongarch64
cmake . -Bbuild-loongarch64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-loongarch64.cmake
ninja -C build-loongarch64
```

O binário resultante será executado apenas em sistemas Linux com arquitetura de CPU LoongArch64.