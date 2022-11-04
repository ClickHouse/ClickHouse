---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
sidebar_position: 67
sidebar_label: "\u5982\u4F55\u5728Linux\u4E0A\u6784\u5EFAClickHouse for AARCH64\uFF08\
  ARM64)"
---

# 如何在Linux上为AARCH64（ARM64）架构构建ClickHouse {#how-to-build-clickhouse-on-linux-for-aarch64-arm64-architecture}

有种情况，当你有台Linux机器，并希望用它来构建 `clickhouse` 的二进制文件，将其运行在另一台AARCH64CPU架构的Linux机器上。这适用于在Linux服务器上运行的持续集成检查。

Aarch64的交叉构建基于 [构建说明](../development/build.md) 让我们先睹为快。

# 安装Clang-8 {#install-clang-8}

按照以下说明操作https://apt.llvm.org/为您的Ubuntu或Debian设置.
例如，在Ubuntu Bionic中，您可以使用以下命令:

``` bash
echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" | sudo tee /etc/apt/sources.list.d/llvm.list
sudo apt-get update
sudo apt-get install clang-8
```

# 安装交叉编译工具集 {#install-cross-compilation-toolset}

``` bash
cd ClickHouse
mkdir -p build-aarch64/cmake/toolchain/linux-aarch64
wget 'https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en' -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
tar xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz -C build-aarch64/cmake/toolchain/linux-aarch64 --strip-components=1
```

# 构建ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-arm64
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-arm64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake
ninja -C build-arm64
```

生成的二进制文件将仅在AARCH64CPU体系架构的Linux上运行。
