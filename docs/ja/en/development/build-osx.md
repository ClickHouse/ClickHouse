---
description: 'macOS システムで ClickHouse をソースからビルドするためのガイド'
sidebar_label: 'macOS で macOS 向けにビルド'
sidebar_position: 15
slug: /development/build-osx
title: 'macOS で macOS 向けにビルド'
keywords: ['MacOS', 'Mac', 'ビルド']
doc_type: 'guide'
---

:::info このビルドガイドは、ClickHouse 本体を変更するコントリビューター向けです。
ClickHouse のソースコードを変更しない場合は、[クイックスタート](https://clickhouse.com/docs/get-started/quick-start)で説明されているとおり、ビルド済みの ClickHouse をインストールできます。
:::

ClickHouse は、macOS 10.15 (Catalina) 以降で、macOS x86&#95;64 (Intel) および arm64 (Apple Silicon) 向けにビルドできます。

サポートされているコンパイラは、Homebrew の Clang のみです。

<div id="install-prerequisites">
  ## 前提条件をインストールする
</div>

まず、共通の[前提条件ドキュメント](developer-instruction.md)を参照してください。

次に、[Homebrew](https://brew.sh/)をインストールして、次を実行します。

その後、次を実行します:

```bash
brew update
brew install ccache cmake ninja libtool gettext llvm lld binutils grep findutils nasm bash rust rustup
```

:::note
Apple ではデフォルトで、大文字と小文字を区別しないファイルシステムが使われています。通常、これはコンパイルには影響しません (特に scratch makes は動作します) が、`git mv` のようなファイル操作で問題になることがあります。
macOS で本格的に開発する場合は、ソースコードを大文字と小文字を区別するディスクボリュームに保存するようにしてください。たとえば、[こちらの手順](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830)を参照してください。
:::

<div id="build-clickhouse">
  ## ClickHouse をビルドする
</div>

ビルドするには、Homebrew の Clang コンパイラを使用する必要があります：

```bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -S . -B build
cmake --build build
# The resulting binary will be created at: build/programs/clickhouse
```

:::note
リンク時に `ld: archive member '/' not a mach-o file in ...` というエラーが発生する場合は、フラグ `-DCMAKE_AR=/opt/homebrew/opt/llvm/bin/llvm-ar` を指定して llvm-ar を使用する必要があることがあります。
:::

<div id="caveats">
  ## 注意点
</div>

`clickhouse-server` を実行する場合は、システムの `maxfiles` 変数を増やしてください。

:::note
`sudo` を使用する必要があります。
:::

そのためには、`/Library/LaunchDaemons/limit.maxfiles.plist` ファイルを次の内容で作成します。

```xml
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

ファイルの権限を適切に設定します:

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

ファイルが正しいことを確認してください:

```bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

ファイルを読み込む (または再起動する) :

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

動作しているかどうかを確認するには、`ulimit -n` または `launchctl limit maxfiles` コマンドを使用します。