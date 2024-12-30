---
slug: /ja/development/build-cross-loongarch
sidebar_position: 70
title: LoongArch64アーキテクチャ向けにLinuxでClickHouseをビルドする方法
sidebar_label: LoongArch64向けLinuxでのビルド
---

執筆時点（2024/03/15）では、loongarch向けのビルドは非常にエクスペリメンタルとされています。すべての機能を有効にできるわけではありません。

これは、Linuxマシンを使用して、LoongArch64 CPUアーキテクチャの別のLinuxマシンで実行する`clickhouse`バイナリをビルドしたい場合のガイドです。これは、Linuxサーバー上で実行される継続的インテグレーションチェックを目的としています。

LoongArch64向けのクロスビルドは、[ビルド手順](../development/build.md)に基づいています。まずはそれに従ってください。

## Clang-18のインストール

UbuntuまたはDebianのセットアップに対して、https://apt.llvm.org/ の手順に従うか、次のコマンドを実行します。
```
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

## ClickHouseのビルド {#build-clickhouse}

ビルドに必要なllvmのバージョンは18.1.0以上でなければなりません。
``` bash
cd ClickHouse
mkdir build-loongarch64
CC=clang-18 CXX=clang++-18 cmake . -Bbuild-loongarch64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-loongarch64.cmake
ninja -C build-loongarch64
```

この結果として得られるバイナリは、LoongArch64 CPUアーキテクチャを搭載したLinuxでのみ実行できます。
