---
toc_priority: 65
toc_title: Build on Mac OS X
---

# How to Build ClickHouse on Mac OS X {#how-to-build-clickhouse-on-mac-os-x}

!!! info "You don't have to build ClickHouse yourself"
    You can install pre-built ClickHouse as described in [Quick Start](https://clickhouse.com/#quick-start).
    Follow `macOS (Intel)` or `macOS (Apple silicon)` installation instructions.

Build should work on x86_64 (Intel) and arm64 (Apple silicon) based macOS 10.15 (Catalina) and higher with Homebrew's vanilla Clang.
It is always recommended to use vanilla `clang` compiler. It is possible to use XCode's `apple-clang` or `gcc` but it's strongly discouraged.

## Install Homebrew {#install-homebrew}

``` bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# ...and follow the printed instructions on any additional steps required to complete the installation.
```

## Install Xcode and Command Line Tools {#install-xcode-and-command-line-tools}

Install the latest [Xcode](https://apps.apple.com/am/app/xcode/id497799835?mt=12) from App Store.

Open it at least once to accept the end-user license agreement and automatically install the required components.

Then, make sure that the latest Command Line Tools are installed and selected in the system:

``` bash
sudo rm -rf /Library/Developer/CommandLineTools
sudo xcode-select --install
```

## Install Required Compilers, Tools, and Libraries {#install-required-compilers-tools-and-libraries}

``` bash
brew update
brew install cmake ninja libtool gettext llvm gcc binutils
```

## Checkout ClickHouse Sources {#checkout-clickhouse-sources}

``` bash
git clone --recursive git@github.com:ClickHouse/ClickHouse.git
# ...alternatively, you can use https://github.com/ClickHouse/ClickHouse.git as the repo URL.
```

## Build ClickHouse {#build-clickhouse}

To build using Homebrew's vanilla Clang compiler (the only **recommended** way):

``` bash
cd ClickHouse
rm -rf build
mkdir build
cd build
cmake -DCMAKE_C_COMPILER=$(brew --prefix llvm)/bin/clang -DCMAKE_CXX_COMPILER=$(brew --prefix llvm)/bin/clang++ -DCMAKE_AR=$(brew --prefix llvm)/bin/llvm-ar -DCMAKE_RANLIB=$(brew --prefix llvm)/bin/llvm-ranlib -DOBJCOPY_PATH=$(brew --prefix llvm)/bin/llvm-objcopy -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
cmake --build . --config RelWithDebInfo
# The resulting binary will be created at: ./programs/clickhouse
```

To build using Xcode's native AppleClang compiler in Xcode IDE (this option is only for development builds and workflows, and is **not recommended** unless you know what you are doing):

``` bash
cd ClickHouse
rm -rf build
mkdir build
cd build
XCODE_IDE=1 ALLOW_APPLECLANG=1 cmake -G Xcode -DCMAKE_BUILD_TYPE=Debug -DENABLE_JEMALLOC=OFF ..
cmake --open .
# ...then, in Xcode IDE select ALL_BUILD scheme and start the building process.
# The resulting binary will be created at: ./programs/Debug/clickhouse
```

To build using Homebrew's vanilla GCC compiler (this option is only for development experiments, and is **absolutely not recommended** unless you really know what you are doing):

``` bash
cd ClickHouse
rm -rf build
mkdir build
cd build
cmake -DCMAKE_C_COMPILER=$(brew --prefix gcc)/bin/gcc-11 -DCMAKE_CXX_COMPILER=$(brew --prefix gcc)/bin/g++-11 -DCMAKE_AR=$(brew --prefix gcc)/bin/gcc-ar-11 -DCMAKE_RANLIB=$(brew --prefix gcc)/bin/gcc-ranlib-11 -DOBJCOPY_PATH=$(brew --prefix binutils)/bin/objcopy -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
cmake --build . --config RelWithDebInfo
# The resulting binary will be created at: ./programs/clickhouse
```

## Caveats {#caveats}

If you intend to run `clickhouse-server`, make sure to increase the system’s maxfiles variable.

!!! info "Note"
    You’ll need to use sudo.

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

[Original article](https://clickhouse.com/docs/en/development/build_osx/) <!--hide-->
