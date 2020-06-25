---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 65
toc_title: "Mac OS X\u3067ClickHouse\u3092\u69CB\u7BC9\u3059\u308B\u65B9\u6CD5"
---

# Mac OS XでClickHouseを構築する方法 {#how-to-build-clickhouse-on-mac-os-x}

ビルドはmac os x10.15(catalina)で動作するはずです)

## ト自作 {#install-homebrew}

``` bash
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## 必要なコンパイラ、ツール、ライブラ {#install-required-compilers-tools-and-libraries}

``` bash
$ brew install cmake ninja libtool gettext
```

## レclickhouse源 {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

または

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git

$ cd ClickHouse
```

## クリックハウスを構築 {#build-clickhouse}

``` bash
$ mkdir build
$ cd build
$ cmake .. -DCMAKE_CXX_COMPILER=`which clang++` -DCMAKE_C_COMPILER=`which clang`
$ ninja
$ cd ..
```

## 警告 {#caveats}

Clickhouse-serverを実行する場合は、システムのmaxfiles変数を増やしてください。

!!! info "メモ"
    Sudoを使用する必要があります。

これを行うには、次のファイルを作成します:

/ライブラリ/LaunchDaemons/制限.マックスファイルplist:

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

次のコマンドを実行します:

``` bash
$ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

再起動しろ

それが動作しているかどうかを確認するには、 `ulimit -n` 司令部

[元の記事](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->
