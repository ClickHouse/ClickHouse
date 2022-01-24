---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 67
toc_title: "Aarch64\u7528\u306ELinux\u4E0A\u3067ClickHouse\u3092\u69CB\u7BC9\u3059\
  \u308B\u65B9\u6CD5(ARM64)"
---

# Aarch64(ARM64)アーキテクチャ用のLinux上でClickHouseを構築する方法 {#how-to-build-clickhouse-on-linux-for-aarch64-arm64-architecture}

これは、Linuxマシンを使用してビルドする場合のためのものです `clickhouse` AARCH64CPUアーキテクチャを持つ別のLinuxマシン上で実行されるバイナリ。 この目的のために継続的インテグレーションをチェックを実行Linuxサーバー

AARCH64のクロスビルドは [ビルド命令](../development/build.md) 先について来い

# Clang-8をインストール {#install-clang-8}

の指示に従ってくださいhttps://apt.llvm.org/あなたのUbuntuまたはDebianのセットアップ用。
たとえば、Ubuntu Bionicでは、次のコマンドを使用できます:

``` bash
echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" | sudo tee /etc/apt/sources.list.d/llvm.list
sudo apt-get update
sudo apt-get install clang-8
```

# クロスコンパイルツールセット {#install-cross-compilation-toolset}

``` bash
cd ClickHouse
mkdir -p build-aarch64/cmake/toolchain/linux-aarch64
wget 'https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en' -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
tar xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz -C build-aarch64/cmake/toolchain/linux-aarch64 --strip-components=1
```

# ビルドClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-arm64
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-arm64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake
ninja -C build-arm64
```

結果のバイナリは、Aarch64CPUアーキテクチャを持つLinux上でのみ実行されます。
