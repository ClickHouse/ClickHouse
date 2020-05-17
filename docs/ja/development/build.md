---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 64
toc_title: "Linux\u4E0A\u3067ClickHouse\u3092\u69CB\u7BC9\u3059\u308B\u65B9\u6CD5"
---

# 開発のためのClickHouseを構築する方法 {#how-to-build-clickhouse-for-development}

次のチュートリアルはUbuntu Linuxシステムに基づいています。
適切な変更により、他のLinuxディストリビューションでも動作するはずです。
サポートされるプラットフォーム:x86\_64およびAArch64。 Power9のサポートは実験的です。

## Git、CMake、Pythonと忍者をインストールします {#install-git-cmake-python-and-ninja}

``` bash
$ sudo apt-get install git cmake python ninja-build
```

古いシステムではcmakeの代わりにcmake3。

## GCC9のインストール {#install-gcc-9}

これを行うにはいくつかの方法があります。

### PPAパッケージからインストール {#install-from-a-ppa-package}

``` bash
$ sudo apt-get install software-properties-common
$ sudo apt-add-repository ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install gcc-9 g++-9
```

### ソースからインスト {#install-from-sources}

見て [utils/ci/build-gcc-from-sources.sh](https://github.com/ClickHouse/ClickHouse/blob/master/utils/ci/build-gcc-from-sources.sh)

## ビルドにGCC9を使用する {#use-gcc-9-for-builds}

``` bash
$ export CC=gcc-9
$ export CXX=g++-9
```

## ﾂつｨﾂ姪"ﾂ債ﾂつｹ {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

または

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git
```

## ビルドClickHouse {#build-clickhouse}

``` bash
$ cd ClickHouse
$ mkdir build
$ cd build
$ cmake ..
$ ninja
$ cd ..
```

実行可能ファイルを作成するには、 `ninja clickhouse`.
これは作成します `programs/clickhouse` 実行可能ファイル `client` または `server` 引数。

# 任意のLinux上でClickHouseを構築する方法 {#how-to-build-clickhouse-on-any-linux}

の構築が必要で以下のコンポーネント:

-   Git（ソースをチェックアウトするためにのみ使用され、ビルドには必要ありません)
-   CMake3.10以降
-   忍者（推奨）または作る
-   C++コンパイラ:gcc9またはclang8以降
-   リンカ:lldまたはgold(古典的なGNU ldは動作しません)
-   Python(LLVMビルド内でのみ使用され、オプションです)

すべてのコンポーネントがインストールされている場合、上記の手順と同じ方法でビルドできます。

Ubuntu Eoanの例:

    sudo apt update
    sudo apt install git cmake ninja-build g++ python
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

OpenSUSEタンブルウィードの例:

    sudo zypper install git cmake ninja gcc-c++ python lld
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

Fedora Rawhideの例:

    sudo yum update
    yum --nogpg install git cmake make gcc-c++ python2
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    make -j $(nproc)

# ClickHouseを構築する必要はありません {#you-dont-have-to-build-clickhouse}

ClickHouseは、事前に構築されたバイナリとパッケージで利用可能です。 バイナリは移植性があり、任意のLinuxフレーバーで実行できます。

これらのために、安定したprestable-試験スリリースして毎にコミットマスターすべてを引きます。

から新鮮なビルドを見つけるには `master`,に行く [コミットページ](https://github.com/ClickHouse/ClickHouse/commits/master) 最初の緑色のチェックマークまたはコミットの近くにある赤い十字をクリックし、 “Details” 右の後にリンク “ClickHouse Build Check”.

# ClickHouse Debianパッケージのビルド方法 {#how-to-build-clickhouse-debian-package}

## GitとPbuilderのインストール {#install-git-and-pbuilder}

``` bash
$ sudo apt-get update
$ sudo apt-get install git python pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## ﾂつｨﾂ姪"ﾂ債ﾂつｹ {#checkout-clickhouse-sources-1}

``` bash
$ git clone --recursive --branch master https://github.com/ClickHouse/ClickHouse.git
$ cd ClickHouse
```

## 解放スクリプトの実行 {#run-release-script}

``` bash
$ ./release
```

[元の記事](https://clickhouse.tech/docs/en/development/build/) <!--hide-->
