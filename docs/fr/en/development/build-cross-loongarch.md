---
description: 'Guide pour compiler ClickHouse depuis le code source pour l''architecture LoongArch64'
sidebar_label: 'Compiler sous Linux pour LoongArch64'
sidebar_position: 35
slug: /development/build-cross-loongarch
title: 'Compiler sous Linux pour LoongArch64'
doc_type: 'guide'
---

ClickHouse prend en charge LoongArch64 à titre expérimental

<div id="build-clickhouse">
  ## Compiler ClickHouse
</div>

La version de LLVM requise pour la compilation doit être supérieure ou égale à 21.1.0.

```bash
cd ClickHouse
mkdir build-loongarch64
cmake . -Bbuild-loongarch64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-loongarch64.cmake
ninja -C build-loongarch64
```

Le binaire obtenu ne s’exécutera que sous Linux sur une architecture CPU LoongArch64.