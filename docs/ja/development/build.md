---
slug: /ja/development/build
sidebar_position: 64
sidebar_label: Linux でのビルド
title: Linux での ClickHouse のビルド方法
description: Linux での ClickHouse のビルド方法
---

対応プラットフォーム:

- x86_64
- AArch64
- PowerPC 64 LE（エクスペリメンタル）
- RISC-V 64（エクスペリメンタル）

## Ubuntu でのビルド

以下のチュートリアルは Ubuntu Linux に基づいています。
適切な変更を加えれば、他の Linux ディストリビューションでも機能するはずです。
開発に推奨される最小の Ubuntu バージョンは 22.04 LTS です。

### 必要条件のインストール {#install-prerequisites}

``` bash
sudo apt-get update
sudo apt-get install git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg
```

### Clang コンパイラのインストールと使用

Ubuntu/Debian では LLVM の自動インストールスクリプトを使用できます。[こちら](https://apt.llvm.org/)をご覧ください。

``` bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

注意: 問題がある場合は、次の方法も使用できます:

```bash
sudo apt-get install software-properties-common
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
```

他の Linux ディストリビューションでは、LLVM の[事前ビルドパッケージ](https://releases.llvm.org/download.html)の利用可能性を確認してください。

2024 年 3 月時点で、clang-17 以上が動作します。
GCC はコンパイラとしてサポートされていません。
特定の Clang バージョンでビルドするには:

:::tip
これは任意です。指示に従って Clang をインストールしたばかりの場合は、
この環境変数を設定する前にインストールされているバージョンを確認してください。
:::

``` bash
export CC=clang-18
export CXX=clang++-18
```

### Rust コンパイラのインストール

まず、公式の [rust ドキュメント](https://www.rust-lang.org/tools/install)で `rustup` をインストールする手順に従ってください。

C++ 依存関係と同様に、ClickHouse はベンダリングを使用してインストールされるものを正確に管理し、サードパーティのサービス（`crates.io` レジストリのような）に依存しないようにしています。

リリースモードでは、最新の rustup toolchain のバージョンであれば、この依存関係で動作するはずですが、
サニタイザを有効にする予定がある場合は、CI で使用されるものと同じ `std` と一致するバージョンを使用する必要があります（crates をベンダリングしています）:

```bash
rustup toolchain install nightly-2024-04-01
rustup default nightly-2024-04-01
rustup component add rust-src
```

### ClickHouse ソースをチェックアウトする {#checkout-clickhouse-sources}

``` bash
git clone --recursive --shallow-submodules git@github.com:ClickHouse/ClickHouse.git
```

または

``` bash
git clone --recursive --shallow-submodules https://github.com/ClickHouse/ClickHouse.git
```

### ClickHouse をビルドする {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build
cmake -S . -B build
cmake --build build  # または: `cd build; ninja`
```

:::tip
`cmake` が利用可能な論理コア数を検出できない場合、ビルドは 1 スレッドで行われます。これを解決するには、`cmake` に `-j` フラグを使用して特定のスレッド数を使うように設定できます。例えば、`cmake --build build -j 16` です。また、フラグを常に設定しなくてもよいように、予め特定のジョブ数でビルドファイルを生成することもできます: `cmake -DPARALLEL_COMPILE_JOBS=16 -S . -B build`、ここで `16` は望むスレッド数です。
:::

実行ファイルを作成するには、`cmake --build build --target clickhouse` を実行します（または: `cd build; ninja clickhouse`）。
これで `build/programs/clickhouse` という実行ファイルが作成され、`client` または `server` 引数とともに使用できます。

## どの Linux でもビルドする {#how-to-build-clickhouse-on-any-linux}

ビルドには次のコンポーネントが必要です:

- Git（ソースのチェックアウトに使用、ビルドには不要）
- CMake 3.20 以上
- コンパイラ: clang-18 以上
- リンカー: lld-17 以上
- Ninja
- Yasm
- Gawk
- rustc

すべてのコンポーネントがインストールされている場合、上記の手順と同様にビルドできます。

OpenSUSE Tumbleweed の例:

``` bash
sudo zypper install git cmake ninja clang-c++ python lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

Fedora Rawhide の例:

``` bash
sudo yum update
sudo yum --nogpg install git cmake make clang python3 ccache lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

## Docker でのビルド

CI ビルドには、`clickhouse/binary-builder` という Docker イメージを使用しています。このイメージには、バイナリとパッケージをビルドするために必要なすべてのものが含まれています。イメージの使用を簡略化するために `docker/packager/packager` というスクリプトがあります:

```bash
# 出力アーティファクト用のディレクトリ定義
output_dir="build_results"
# シンプルなビルド
./docker/packager/packager --package-type=binary --output-dir "$output_dir"
# debian パッケージのビルド
./docker/packager/packager --package-type=deb --output-dir "$output_dir"
# デフォルトでは、debian パッケージはスリムな LTO を使用するため、ビルドを高速化するためにこれをオーバーライドできます
CMAKE_FLAGS='-DENABLE_THINLTO=' ./docker/packager/packager --package-type=deb --output-dir "./$(git rev-parse --show-cdup)/build_results"
```
