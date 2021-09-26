---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 66
toc_title: "Mac OS X\u7528\u306ELinux\u4E0A\u3067ClickHouse\u3092\u69CB\u7BC9\u3059\
  \u308B\u65B9\u6CD5"
---

# Mac OS X用のLinux上でClickHouseを構築する方法 {#how-to-build-clickhouse-on-linux-for-mac-os-x}

これは、Linuxマシンを使用してビルドする場合のためのものです `clickhouse` これは、Linuxサーバー上で実行される継続的な統合チェックを目的としています。 Mac OS X上でClickHouseを直接ビルドする場合は、次の手順に進みます [別の命令](build-osx.md).

Mac OS X用のクロスビルドは [ビルド命令](build.md) 先について来い

# Clang-8をインストール {#install-clang-8}

の指示に従ってくださいhttps://apt.llvm.org/あなたのUbuntuまたはDebianのセットアップ用。
例えば、コマンドバイオニックのような:

``` bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

# クロスコンパイルツールセット {#install-cross-compilation-toolset}

インストール先のパスを覚えてみましょう `cctools` として${CCTOOLS}

``` bash
mkdir ${CCTOOLS}

git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
INSTALLPREFIX=${CCTOOLS} ./build.sh
./install.sh
cd ..

git clone https://github.com/tpoechtrager/cctools-port.git
cd cctools-port/cctools
./configure --prefix=${CCTOOLS} --with-libtapi=${CCTOOLS} --target=x86_64-apple-darwin
make install
```

また、作業ツリーにmacOS X SDKをダウンロードする必要があります。

``` bash
cd ClickHouse
wget 'https://github.com/phracker/MacOSX-SDKs/releases/download/10.15/MacOSX10.15.sdk.tar.xz'
mkdir -p build-darwin/cmake/toolchain/darwin-x86_64
tar xJf MacOSX10.15.sdk.tar.xz -C build-darwin/cmake/toolchain/darwin-x86_64 --strip-components=1
```

# ビルドClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-osx
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-osx -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake \
    -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar \
    -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib \
    -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld
ninja -C build-osx
```

結果のバイナリはmach-O実行可能フォーマットを持ち、Linux上で実行することはできません。
