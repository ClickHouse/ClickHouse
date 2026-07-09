---
description: '关于在 macOS 系统上从源码构建 ClickHouse 的指南'
sidebar_label: '在 macOS 上构建 macOS 版'
sidebar_position: 15
slug: /development/build-osx
title: '在 macOS 上构建 macOS 版'
keywords: ['MacOS', 'Mac', 'build']
doc_type: 'guide'
---

:::info 本构建指南适用于修改 ClickHouse 本身的贡献者。
如果不需要更改 ClickHouse 源代码，可以按照 [快速入门](https://clickhouse.com/docs/get-started/quick-start) 中的说明安装预编译的 ClickHouse。
:::

ClickHouse 可在 macOS x86&#95;64 (Intel) 和 arm64 (Apple Silicon) 上编译，要求 macOS 版本为 10.15 (Catalina) 或更高。

编译器仅支持通过 Homebrew 安装的 Clang。

<div id="install-prerequisites">
  ## 安装前置条件
</div>

首先，请参阅通用的[前置条件文档](developer-instruction.md)。

接着，安装 [Homebrew](https://brew.sh/) 并运行

然后运行：

```bash
brew update
brew install ccache cmake ninja libtool gettext llvm lld binutils grep findutils nasm bash rust rustup
```

:::note
Apple 默认使用不区分大小写的文件系统。虽然这通常不会影响编译 (尤其是全新构建一般都能正常工作) ，但可能会给 `git mv` 这类文件操作带来困扰。
如果要在 macOS 上进行正式开发，请确保源代码存放在区分大小写的磁盘卷上，例如可参考[这些说明](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830)。
:::

<div id="build-clickhouse">
  ## 构建 ClickHouse
</div>

构建时必须使用 Homebrew 的 Clang 编译器：

```bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -S . -B build
cmake --build build
# The resulting binary will be created at: build/programs/clickhouse
```

:::note
如果你在链接时遇到 `ld: archive member '/' not a mach-o file in ...` 错误，可能需要
通过设置 flag &#96;-DCMAKE&#95;AR=/opt/homebrew/opt/llvm/bin/llvm-ar&#96;&#96; 来使用 llvm-ar。
:::

<div id="caveats">
  ## 注意事项
</div>

如果你打算运行 `clickhouse-server`，请务必调高系统的 `maxfiles` 变量。

:::note
你需要使用 sudo。
:::

为此，请创建 `/Library/LaunchDaemons/limit.maxfiles.plist` 文件，内容如下：

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

为文件设置正确的权限：

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

确认该文件是否正确：

```bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

加载此文件 (或重启) ：

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

要检查是否生效，请使用 `ulimit -n` 或 `launchctl limit maxfiles` 命令。