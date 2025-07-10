---
description: 'Guide for building ClickHouse from source on macOS systems'
sidebar_label: 'Build on macOS for macOS'
sidebar_position: 15
slug: /development/build-osx
title: 'Build on macOS for macOS'
keywords: ['MacOS', 'Mac', 'build']
---

# How to Build ClickHouse on macOS for macOS

:::info You don't need to build ClickHouse yourself!
You can install pre-built ClickHouse as described in [Quick Start](https://clickhouse.com/#quick-start).
:::

ClickHouse can be compiled on macOS x86_64 (Intel) and arm64 (Apple Silicon) using on macOS 10.15 (Catalina) or higher.

As compiler, only Clang from homebrew is supported.

## Install Prerequisites {#install-prerequisites}

First, see the generic [prerequisites documentation](developer-instruction.md).

Next, install [Homebrew](https://brew.sh/) and run

Then run:

```bash
brew update
brew install ccache cmake ninja libtool gettext llvm binutils grep findutils nasm bash
```

:::note
Apple uses a case-insensitive file system by default. While this usually does not affect compilation (especially scratch makes will work), it can confuse file operations like `git mv`.
For serious development on macOS, make sure that the source code is stored on a case-sensitive disk volume, e.g. see [these instructions](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830).
:::

## Build ClickHouse {#build-clickhouse}

To build you must use Homebrew's Clang compiler:

```bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -S . -B build
cmake --build build
# The resulting binary will be created at: build/programs/clickhouse
```

:::note
If you are running into `ld: archive member '/' not a mach-o file in ...` errors during linking, you may need
to use llvm-ar by setting flag `-DCMAKE_AR=/opt/homebrew/opt/llvm/bin/llvm-ar`.
:::

## Caveats {#caveats}

If you intend to run `clickhouse-server`, make sure to increase the system's `maxfiles` variable.

:::note
You'll need to use sudo.
:::

To do so, create the `/Library/LaunchDaemons/limit.maxfiles.plist` file with the following content:

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

Give the file correct permissions:

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Validate that the file is correct:

```bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

Load the file (or reboot):

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

To check if it's working, use the `ulimit -n` or `launchctl limit maxfiles` commands.
