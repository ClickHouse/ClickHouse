---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 66
toc_title: "Mac OS X\u7528\u306ELinux\u3067ClickHouse\u3092\u69CB\u7BC9\u3059\u308B\
  \u65B9\u6CD5"
---

# Mac OS X用のLinuxでClickHouseを構築する方法 {#how-to-build-clickhouse-on-linux-for-mac-os-x}

これは、linuxマシンがあり、それを使ってビルドしたい場合のためのものです `clickhouse` OS X上で実行されるバイナリこれは、Linuxサーバー上で実行される継続的な統合チェックを目的としています。 Mac OS XでClickHouseを直接ビルドする場合は、次の手順に進んでください [別の命令](build-osx.md).

Mac OS X用のクロスビルドは、以下に基づいています。 [ビルド手順](build.md)、最初にそれらに続きなさい。

# インストールclang-8 {#install-clang-8}

以下の指示に従ってくださいhttps://apt.llvm.org/あなたのubuntuやdebianの設定のために.
例えば、コマンドバイオニックのような:

``` bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

# クロスコンパイルツールセット {#install-cross-compilation-toolset}

いっしょうにパスを設置 `cctools` として${CCTOOLS}

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

また、macos x sdkを作業ツリーにダウンロードする必要があります。

``` bash
cd ClickHouse
wget 'https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz'
mkdir -p build-darwin/cmake/toolchain/darwin-x86_64
tar xJf MacOSX10.14.sdk.tar.xz -C build-darwin/cmake/toolchain/darwin-x86_64 --strip-components=1
```

# クリックハウスを構築 {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-osx
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-osx -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake \
    -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar \
    -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib \
    -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld
ninja -C build-osx
```

結果のバイナリはmach-o実行可能フォーマットを持ち、linuxでは実行できません。
