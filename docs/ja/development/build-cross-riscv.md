---
slug: /ja/development/build-cross-riscv
sidebar_position: 68
title: LinuxでRISC-V 64アーキテクチャ用ClickHouseをビルドする方法
sidebar_label: RISC-V 64用Linuxでのビルド
---

執筆時点（2021年11月11日）では、RISC-V用のビルドは非常にエクスペリメンタルと考えられています。すべての機能を有効にすることはできません。

これは、Linuxマシンを持っていて、そのマシンでRISC-V 64 CPUアーキテクチャを備えた別のLinuxマシンで実行する`clickhouse`バイナリをビルドしたい場合の手順です。これはLinuxサーバーで実行される継続的インテグレーションチェック用に意図されています。

RISC-V 64用のクロスビルドは[ビルド手順](../development/build.md)に基づいているため、まずそれに従ってください。

## Clang-18のインストール

あなたのUbuntuまたはDebian環境に合わせて、https://apt.llvm.org/ の指示に従ってください。または以下のコマンドを実行します。
```
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

## ClickHouseをビルドする {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-riscv64
CC=clang-18 CXX=clang++-18 cmake . -Bbuild-riscv64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-riscv64.cmake -DGLIBC_COMPATIBILITY=OFF -DENABLE_LDAP=OFF  -DOPENSSL_NO_ASM=ON -DENABLE_JEMALLOC=ON -DENABLE_PARQUET=OFF -DENABLE_GRPC=OFF -DENABLE_HDFS=OFF -DENABLE_MYSQL=OFF
ninja -C build-riscv64
```

生成されたバイナリはRISC-V 64 CPUアーキテクチャを持つLinuxでのみ実行可能です。
