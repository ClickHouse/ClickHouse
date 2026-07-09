---
description: 'AARCH64 アーキテクチャ向けにソースコードから ClickHouse をビルドするためのガイド'
sidebar_label: 'Linux で AARCH64 向けにビルド'
sidebar_position: 25
slug: /development/build-cross-arm
title: 'Linux で AARCH64 向けに ClickHouse をビルドする方法'
doc_type: 'guide'
---

Aarch64 マシン上で Aarch64 向けに ClickHouse をビルドする場合、特別な手順は必要ありません。

x86 Linux マシン上で AArch64 向けに ClickHouse をクロスコンパイルするには、`cmake` に次のフラグを渡します: `-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`