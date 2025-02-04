---
slug: /en/development/build-osx
sidebar_position: 65
sidebar_label: Build on macOS
title: How to Build ClickHouse on macOS
description: How to build ClickHouse on macOS for macOS
---

:::info You don't have to build ClickHouse yourself!
You can install pre-built ClickHouse as described in [Quick Start](https://clickhouse.com/#quick-start). Follow **macOS (Intel)** or **macOS (Apple silicon)** installation instructions.
:::

The build works on x86_64 (Intel) and arm64 (Apple Silicon) based on macOS 10.15 (Catalina) or higher with Homebrew's vanilla Clang.

:::note
It is also possible to compile with Apple's XCode `apple-clang`, but it's strongly discouraged.
:::

## Install Homebrew {#install-homebrew}

First install [Homebrew](https://brew.sh/)

## For Apple's Clang (discouraged): Install XCode and Command Line Tools {#install-xcode-and-command-line-tools}

Install the latest [XCode](https://apps.apple.com/am/app/xcode/id497799835?mt=12) from App Store.

Open it at least once to accept the end-user license agreement and automatically install the required components.

Then, make sure that the latest Command Line Tools are installed and selected in the system:

``` bash
sudo rm -rf /Library/Developer/CommandLineTools
sudo xcode-select --install
```

## Install Required Compilers, Tools, and Libraries {#install-required-compilers-tools-and-libraries}

``` bash
brew update
brew install ccache cmake ninja libtool gettext llvm gcc binutils grep findutils nasm
```

## Checkout ClickHouse Sources {#checkout-clickhouse-sources}

``` bash
git clone --recursive git@github.com:ClickHouse/ClickHouse.git
# ...alternatively, you can use https://github.com/ClickHouse/ClickHouse.git as the repo URL.
```

Apple uses a case-insensitive file system by default. While this usually does not affect compilation (especially scratch makes will work), it can confuse file operations like `git mv`.
For serious development on macOS, make sure that the source code is stored on a case-sensitive disk volume, e.g. see [these instructions](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830).

## Build ClickHouse {#build-clickhouse}

To build using Homebrew's vanilla Clang compiler (the only **recommended** way):

``` bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_C_COMPILER=$(brew --prefix llvm)/bin/clang -DCMAKE_CXX_COMPILER=$(brew --prefix llvm)/bin/clang++ -S . -B build
cmake --build build
# The resulting binary will be created at: build/programs/clickhouse
```

To build using XCode native AppleClang compiler in XCode IDE (this option is only for development builds and workflows, and is **not recommended** unless you know what you are doing):

``` bash
cd ClickHouse
rm -rf build
mkdir build
cd build
XCODE_IDE=1 ALLOW_APPLECLANG=1 cmake -G Xcode -DCMAKE_BUILD_TYPE=Debug -DENABLE_JEMALLOC=OFF ..
cmake --open .
# ...then, in XCode IDE select ALL_BUILD scheme and start the building process.
# The resulting binary will be created at: ./programs/Debug/clickhouse
```

## Caveats {#caveats}

If you intend to run `clickhouse-server`, make sure to increase the system’s `maxfiles` variable.

:::note
You’ll need to use sudo.
:::

To do so, create the `/Library/LaunchDaemons/limit.maxfiles.plist` file with the following content:

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

Give the file correct permissions:

``` bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Validate that the file is correct:

``` bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

Load the file (or reboot):

``` bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

To check if it’s working, use the `ulimit -n` or `launchctl limit maxfiles` commands.

## Running ClickHouse server

``` bash
cd ClickHouse
./build/programs/clickhouse-server --config-file ./programs/server/config.xml
```
