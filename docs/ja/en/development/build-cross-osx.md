---
description: 'Linux から macOS 向けに ClickHouse をクロスコンパイルするためのガイド'
sidebar_label: 'Linux 上で macOS 向けにビルド'
sidebar_position: 20
slug: /development/build-cross-osx
title: 'Linux 上で macOS 向けにビルド'
doc_type: 'guide'
---

これは、Linux マシンを使って OS X 上で動作する `clickhouse` 実行バイナリをビルドしたい場合のためのガイドです。
主なユースケースは、Linux マシン上で実行される継続的インテグレーションのチェックです。
ClickHouse を macOS 上で直接ビルドしたい場合は、[ネイティブビルド手順](../development/build-osx.md)に進んでください。

macOS 向けのクロスビルドは [ビルド手順](../development/build.md) に基づいているため、まず先にそちらに従ってください。

以下のセクションでは、`x86_64` の macOS 向けに ClickHouse をビルドする手順を順を追って説明します。
ARM アーキテクチャを対象とする場合は、`x86_64` の出現箇所をすべて `aarch64` に置き換えるだけです。
たとえば、手順全体で `x86_64-apple-darwin` を `aarch64-apple-darwin` に置き換えてください。

<div id="install-cross-compilation-toolset">
  ## クロスコンパイル用ツールセットをインストールする
</div>

`cctools` をインストールしたパスは `${CCTOOLS}` として覚えておきましょう

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

また、macOS X SDK をワーキングツリーにダウンロードする必要があります。

```bash
cd ClickHouse/cmake/toolchain/darwin-x86_64
curl -L 'https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz' | tar xJ --strip-components=1
```

<div id="build-clickhouse">
  ## ClickHouseをビルドする
</div>

```bash
cd ClickHouse
mkdir build-darwin
cd build-darwin
cmake -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=${CCTOOLS}/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake ..
ninja
```

生成されるバイナリはMach-O形式の実行ファイルとなるため、Linuxでは実行できません。