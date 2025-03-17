---
slug: /ja/development/build-osx
sidebar_position: 65
sidebar_label: macOSでビルド
title: macOSでClickHouseをビルドする方法
description: macOS向けにClickHouseをビルドする方法
---

:::info ClickHouseを自分でビルドする必要はありません！
[クイックスタート](https://clickhouse.com/#quick-start)に記載されているように、事前にビルドされたClickHouseをインストールできます。**macOS (Intel)** または **macOS (Apple silicon)** のインストール手順に従ってください。
:::

このビルドはmacOS 10.15 (Catalina)以降のx86_64 (Intel)およびarm64 (Apple Silicon)で、Homebrewの標準Clangを使用して動作します。

:::note
AppleのXCode `apple-clang`でもコンパイルすることが可能ですが、これは強く推奨されません。
:::

## Homebrewをインストールする {#install-homebrew}

まず、[Homebrew](https://brew.sh/)をインストールしてください。

## AppleのClangを使用する場合（非推奨）：XCodeおよびコマンドラインツールのインストール {#install-xcode-and-command-line-tools}

App Storeから最新の[XCode](https://apps.apple.com/am/app/xcode/id497799835?mt=12)をインストールします。

少なくとも一度開いて、エンドユーザーライセンス契約に同意し、自動的に必要なコンポーネントをインストールします。

次に、最新のコマンドラインツールがシステムにインストールされて選択されていることを確認してください:

``` bash
sudo rm -rf /Library/Developer/CommandLineTools
sudo xcode-select --install
```

## 必要なコンパイラ、ツール、およびライブラリのインストール {#install-required-compilers-tools-and-libraries}

``` bash
brew update
brew install ccache cmake ninja libtool gettext llvm gcc binutils grep findutils nasm
```

## ClickHouseソースのチェックアウト {#checkout-clickhouse-sources}

``` bash
git clone --recursive git@github.com:ClickHouse/ClickHouse.git
# ...または、https://github.com/ClickHouse/ClickHouse.git をリポジトリURLとして使用することもできます。
```

Appleはデフォルトでケースインセンシティブなファイルシステムを使用しています。通常、これはコンパイルに影響はありません（特にスクラッチメイクは問題なく動作します）が、`git mv`のようなファイル操作を混乱させることがあります。macOSで本格的な開発を行う場合、ソースコードがケースセンシティブなディスクボリュームに保存されていることを確認してください。たとえば、[これらの手順](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830)を参照してください。

## ClickHouseをビルドする {#build-clickhouse}

Homebrewの標準Clangコンパイラを使用してビルドする場合（唯一の**推奨される**方法）:

``` bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_C_COMPILER=$(brew --prefix llvm)/bin/clang -DCMAKE_CXX_COMPILER=$(brew --prefix llvm)/bin/clang++ -S . -B build
cmake --build build
# 結果として生成されるバイナリは次の場所に作成されます: build/programs/clickhouse
```

XCodeのネイティブAppleClangコンパイラを使用してXCode IDEでビルドする場合（このオプションは開発ビルドとワークフローのみであり、あなたが何をしているかについて理解がある場合を除き、**推奨されません**）:

``` bash
cd ClickHouse
rm -rf build
mkdir build
cd build
XCODE_IDE=1 ALLOW_APPLECLANG=1 cmake -G Xcode -DCMAKE_BUILD_TYPE=Debug -DENABLE_JEMALLOC=OFF ..
cmake --open .
# ...次に、XCode IDEでALL_BUILDスキームを選択し、ビルドプロセスを開始します。
# 結果として生成されるバイナリは次の場所に作成されます: ./programs/Debug/clickhouse
```

## 注意事項 {#caveats}

`clickhouse-server`を実行する予定がある場合、システムの`maxfiles`変数を増やす必要があります。

:::note
`sudo`を使用する必要があります。
:::

そのためには、次の内容で`/Library/LaunchDaemons/limit.maxfiles.plist`ファイルを作成します:

``` xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
        "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>limit.maxfiles</string>
    <key>ProgramArguments</key>
    <array>
      <string>launchctl</string>
      <string>limit</string>
      <string>maxfiles</string>
      <string>524288</string>
      <string>524288</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>ServiceIPC</key>
    <false/>
  </dict>
</plist>
```

ファイルに正しい権限を与えます:

``` bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

ファイルが正しいことを確認します:

``` bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

ファイルをロードします（または再起動します）:

``` bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

機能しているか確認するには、`ulimit -n`または`launchctl limit maxfiles`コマンドを使用します。

## ClickHouseサーバーを実行する

``` bash
cd ClickHouse
./build/programs/clickhouse-server --config-file ./programs/server/config.xml
```
