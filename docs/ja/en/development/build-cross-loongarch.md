---
description: 'LoongArch64 アーキテクチャ向けにソースコードから ClickHouse をビルドするためのガイド'
sidebar_label: 'LoongArch64 向け Linux でのビルド'
sidebar_position: 35
slug: /development/build-cross-loongarch
title: 'LoongArch64 向け Linux でのビルド'
doc_type: 'guide'
---

ClickHouse は LoongArch64 を実験的にサポートしています

<div id="build-clickhouse">
  ## ClickHouse をビルドする
</div>

ビルドに必要な llvm のバージョンは 21.1.0 以上です。

```bash
cd ClickHouse
mkdir build-loongarch64
cmake . -Bbuild-loongarch64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-loongarch64.cmake
ninja -C build-loongarch64
```

生成されたバイナリは、LoongArch64 CPUアーキテクチャのLinux上でのみ実行できます。