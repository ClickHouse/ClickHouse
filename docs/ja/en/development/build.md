---
description: 'Linux システムで ClickHouse をソースコードからビルドするためのステップごとのガイド'
sidebar_label: 'Linux でビルド'
sidebar_position: 10
slug: /development/build
title: 'Linux で ClickHouse をビルドする方法'
doc_type: 'guide'
---

:::info このビルドガイドは、ClickHouse 自体に変更を加えるコントリビューター向けです。
ClickHouse のソースコードを変更しない場合は、[クイックスタート](https://clickhouse.com/docs/get-started/quick-start) で説明されているように、事前ビルド済みの ClickHouse をインストールできます。
:::

ClickHouse は次のプラットフォームでビルドできます。

* x86&#95;64
* AArch64
* PowerPC 64 LE (実験的) 
* s390/x (実験的) 
* RISC-V 64 (実験的)

<div id="assumptions">
  ## 前提条件
</div>

以下のチュートリアルは Ubuntu Linux をベースにしていますが、適切に変更すれば他の Linux ディストリビューションでも動作するはずです。
開発用として推奨される Ubuntu の最小バージョンは 24.04 LTS です。

このチュートリアルでは、ClickHouseリポジトリとすべてのサブモジュールをローカルにチェックアウト済みであることを前提としています。

<div id="install-prerequisites">
  ## 前提条件をインストールする
</div>

まず、一般的な[前提条件のドキュメント](developer-instruction.md)を参照してください。

ClickHouse のビルドには CMake と Ninja を使用します。

必要に応じて、ビルド時にすでにコンパイル済みの object file を再利用できるよう、ccache をインストールできます。

```bash
sudo apt-get update
sudo apt-get install build-essential git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg
```

<div id="install-the-clang-compiler">
  ## Clang コンパイラをインストールする
</div>

Ubuntu/Debian に Clang をインストールするには、[こちら](https://apt.llvm.org/) にある LLVM の自動インストールスクリプトを使用します。

```bash
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 21
```

その他のLinuxディストリビューションでは、LLVMの[ビルド済みパッケージ](https://releases.llvm.org/download.html)をインストールできるか確認してください。

2026年2月時点では、Clang 21以降が必要です。
GCCやその他のコンパイラはサポートされていません。

<div id="install-the-rust-compiler-optional">
  ## Rust コンパイラをインストールする (任意)
</div>

:::note
Rust は ClickHouse のオプションの依存関係です。
Rust がインストールされていない場合、ClickHouse の一部機能はコンパイル時に省略されます。
:::

まず、公式の [Rust ドキュメント](https://www.rust-lang.org/tools/install) の手順に従って、`rustup` をインストールしてください。

C++ の依存関係と同様に、ClickHouse は、何をインストールするかを厳密に制御し、サードパーティサービス (`crates.io` レジストリなど) への依存を避けるために、ベンダリングを使用しています。

リリースモードでは、通常、比較的新しい rustup ツールチェーンであればどのバージョンでもこれらの依存関係で動作するはずですが、サニタイザを有効にする予定がある場合は、CI で使用されているものとまったく同じ `std` に一致するバージョンを使う必要があります (そのために crate をベンダリングしています) 。

```bash
rustup toolchain install nightly-2026-03-22
rustup default nightly-2026-03-22
rustup component add rust-src
```

<div id="build-clickhouse">
  ## ClickHouse をビルドする
</div>

すべてのビルド成果物を格納するため、`ClickHouse` 内に `build` という別のディレクトリを作成することを推奨します。

```sh
mkdir build
cd build
```

異なるビルドタイプごとに、複数のディレクトリ (例: `build_release`、`build_debug` など) を用意できます。

任意: 複数のバージョンのコンパイラがインストールされている場合は、使用するコンパイラを明示的に指定することもできます。

```sh
export CC=clang-21
export CXX=clang++-21
```

開発用途では、デバッグビルドを推奨します。
リリースビルドと比べると、コンパイラの最適化レベル (`-O`) が低いため、デバッグしやすくなります。
また、`LOGICAL_ERROR` 型の内部例外は、穏当に処理されて失敗するのではなく、即座にクラッシュします。

```sh
cmake -D CMAKE_BUILD_TYPE=Debug ..
```

:::note
gdb などのデバッガーを使用する場合は、上記のコマンドに `-D DEBUG_O_LEVEL="0"` を追加して、変数の表示やアクセスの妨げになる可能性があるコンパイラの最適化をすべて無効にします。
:::

ビルドするには、ninja を実行します:

```sh
ninja clickhouse
```

すべてのバイナリ (ユーティリティとテストを含む) をビルドするには、パラメーターを指定せずに ninja を実行します。

```sh
ninja
```

`-j` パラメータを使用して、並列ビルドジョブの数を制御できます。

```sh
ninja -j 1 clickhouse
```

:::note
`clickhouse-server`、`clickhouse-client`、および同様のバイナリは、ビルド完了後、`programs/` ディレクトリ内で `clickhouse` 実行ファイルを指すシンボリックリンクになっています。

:::tip
CMake には、上記のコマンドに対応するショートカットが用意されています。

```sh
cmake -S . -B build  # configure build, run from repository top-level directory
cmake --build build  # compile
```

:::

<div id="running-the-clickhouse-executable">
  ## ClickHouse 実行可能ファイルの実行
</div>

ビルドが正常に完了すると、実行可能ファイルは `ClickHouse/<build_dir>/programs/` にあります。

ClickHouse server は、現在のディレクトリから設定ファイル `config.xml` を探します。
また、コマンドラインで `-C` を指定して設定ファイルを指定することもできます。

`clickhouse-client` で ClickHouse server に接続するには、別のターミナルを開き、`ClickHouse/build/programs/` に移動して `./clickhouse client` を実行します。

macOS または FreeBSD で `Connection refused` メッセージが表示された場合は、ホストアドレス 127.0.0.1 を指定してみてください:

```bash
clickhouse client --host 127.0.0.1
```

<div id="advanced-options">
  ## 詳細オプション
</div>

<div id="minimal-build">
  ### 最小ビルド
</div>

サードパーティライブラリによる機能が不要な場合は、ビルドをさらに高速化できます。

```sh
cmake -DENABLE_LIBRARIES=OFF
```

問題が発生した場合は、自力で対処する必要があります...

Rust にはインターネット接続が必要です。Rust サポートを無効にするには:

```sh
cmake -DENABLE_RUST=OFF
```

<div id="running-the-clickhouse-executable-1">
  ### ClickHouse 実行可能ファイルの実行
</div>

システムにインストールされている本番環境用の ClickHouse 実行可能ファイルを、コンパイルした ClickHouse 実行可能ファイルに置き換えることができます。
そのためには、公式サイトの手順に従って、お使いのマシンに ClickHouse をインストールします。
次に、以下を実行します。

```bash
sudo service clickhouse-server stop
sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
sudo service clickhouse-server start
```

`clickhouse-client`、`clickhouse-server` などは、共通の `clickhouse` バイナリへのシンボリックリンクである点に注意してください。

また、システムにインストールされている ClickHouse パッケージの設定ファイルを使って、独自にビルドした ClickHouse バイナリを実行することもできます。

```bash
sudo service clickhouse-server stop
sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

<div id="building-on-any-linux">
  ### 任意の Linux 上でのビルド
</div>

OpenSUSE Tumbleweed に前提条件をインストールします:

```bash
sudo zypper install git cmake ninja clang-c++ python lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

Fedora Rawhide で前提条件をインストールします:

```bash
sudo yum update
sudo yum --nogpg install git cmake make clang python3 ccache lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

<div id="building-in-docker">
  ### Docker でのビルド
</div>

以下を使用すると、CI に近い環境で任意のビルドをローカルで実行できます。

```bash
python -m ci.praktika run "BUILD_JOB_NAME"
```

ここで BUILD&#95;JOB&#95;NAME は、CI レポートに表示される job 名です。たとえば、&quot;Build (arm&#95;release)&quot;、&quot;Build (amd&#95;debug)&quot; です。

このコマンドは、必要な dependencies をすべて含む適切な Docker イメージ `clickhouse/binary-builder` を取得し、
その中で build script `./ci/jobs/build_clickhouse.py` を実行します。

build output は `./ci/tmp/` に配置されます。

これは AMD と ARM の両方の Architecture で動作し、追加で必要なのは `requests` モジュールが利用可能な Python と Docker だけです。