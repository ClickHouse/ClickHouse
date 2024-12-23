---
slug: /ja/development/build-cross-osx
sidebar_position: 66
title: Linux上でmacOS用ClickHouseをビルドする方法
sidebar_label: Linux上でmacOS用にビルド
---

これは、Linuxマシンを使用してOS Xで実行される`clickhouse`バイナリをビルドしたい場合の手順です。 これはLinuxサーバー上で実行される継続的なインテグレーションチェックを目的としています。 macOS上で直接ClickHouseをビルドしたい場合は、[別の手順](../development/build-osx.md)に進んでください。

macOS用のクロスビルドは、[ビルド手順](../development/build.md)に基づいていますので、最初にそれに従ってください。

以下のセクションでは、`x86_64` macOS用にClickHouseをビルドするための手順を説明します。ARMアーキテクチャを対象とする場合は、すべての`x86_64`を`aarch64`に置き換えてください。たとえば、手順内の`x86_64-apple-darwin`を`aarch64-apple-darwin`に置き換えます。

## clang-18をインストールする

UbuntuまたはDebianセットアップに合ったhttps://apt.llvm.org/の指示に従ってください。 たとえば、Bionicのコマンドは以下のようになります:

```bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-17 main" >> /etc/apt/sources.list
sudo apt-get install clang-18
```

## クロスコンパイルツールセットをインストールする {#install-cross-compilation-toolset}

`cctools`をインストールするパスを${CCTOOLS}として記憶しておきましょう

```bash
mkdir ~/cctools
export CCTOOLS=$(cd ~/cctools && pwd)
cd ${CCTOOLS}

git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
git checkout 15dfc2a8c9a2a89d06ff227560a69f5265b692f9
INSTALLPREFIX=${CCTOOLS} ./build.sh
./install.sh
cd ..

git clone https://github.com/tpoechtrager/cctools-port.git
cd cctools-port/cctools
git checkout 2a3e1c2a6ff54a30f898b70cfb9ba1692a55fad7
./configure --prefix=$(readlink -f ${CCTOOLS}) --with-libtapi=$(readlink -f ${CCTOOLS}) --target=x86_64-apple-darwin
make install
```

また、macOS X SDKを作業ツリーにダウンロードする必要があります。

```bash
cd ClickHouse/cmake/toolchain/darwin-x86_64
curl -L 'https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz' | tar xJ --strip-components=1
```

## ClickHouseをビルドする {#build-clickhouse}

```bash
cd ClickHouse
mkdir build-darwin
cd build-darwin
CC=clang-18 CXX=clang++-18 cmake -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=${CCTOOLS}/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake ..
ninja
```

生成されるバイナリはMach-O実行形式で、Linux上では実行できません。
