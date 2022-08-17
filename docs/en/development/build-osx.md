---
toc_priority: 65
toc_title: Build on Mac OS X
---

# How to Build ClickHouse on Mac OS X {#how-to-build-clickhouse-on-mac-os-x}

Build should work on x86_64 (Intel) and arm64 (Apple Silicon) based macOS 10.15 (Catalina) and higher with recent Xcode's native AppleClang, or Homebrew's vanilla Clang or GCC compilers.

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

Reboot.

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

To build using Xcode's native AppleClang compiler:

``` bash
cd ClickHouse
rm -rf build
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
cmake --build . --config RelWithDebInfo
cd ..
```

To build using Homebrew's vanilla Clang compiler:

``` bash
cd ClickHouse
rm -rf build
mkdir build
cd build
cmake -DCMAKE_C_COMPILER=$(brew --prefix llvm)/bin/clang -DCMAKE_CXX_COMPILER=$(brew --prefix llvm)/bin/clang++ -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
cmake --build . --config RelWithDebInfo
cd ..
```

To build using Homebrew's vanilla GCC compiler:

``` bash
cd ClickHouse
rm -rf build
mkdir build
cd build
cmake -DCMAKE_C_COMPILER=$(brew --prefix gcc)/bin/gcc-10 -DCMAKE_CXX_COMPILER=$(brew --prefix gcc)/bin/g++-10 -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
cmake --build . --config RelWithDebInfo
cd ..
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

Execute the following command:

``` bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Reboot.

To check if it’s working, you can use `ulimit -n` command.

## Run ClickHouse server:

```
cd ClickHouse
./build/programs/clickhouse-server --config-file ./programs/server/config.xml
```

[Original article](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->
