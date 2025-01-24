---
slug: /ja/development/build-cross-arm
sidebar_position: 67
title: Linux上でAARCH64 (ARM64)アーキテクチャ用にClickHouseをビルドする方法
sidebar_label: AARCH64 (ARM64)用ビルド
---

AArch64マシンを使用しており、AArch64用にClickHouseをビルドしたい場合は、通常通りにビルドしてください。

x86_64マシンを使用しており、AArch64用にクロスコンパイルしたい場合は、`cmake`に以下のフラグを追加してください: `-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`
