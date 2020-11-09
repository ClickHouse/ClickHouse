---
toc_priority: 65
toc_title: Build on Mac OS X
---

# How to Build ClickHouse on Mac OS X {#how-to-build-clickhouse-on-mac-os-x}

Build should work on Mac OS X 10.15 (Catalina).

## Install Homebrew {#install-homebrew}

``` bash
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## Install Required Compilers, Tools, and Libraries {#install-required-compilers-tools-and-libraries}

``` bash
$ brew install cmake ninja libtool gettext llvm
```

## Checkout ClickHouse Sources {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

or

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git

$ cd ClickHouse
```

## Build ClickHouse {#build-clickhouse}

``` bash
$ mkdir build
$ cd build
$ export PATH=/usr/local/opt/llvm/bin:$PATH
$ export LDFLAGS="-L/usr/local/opt/llvm/lib"
$ export CPPFLAGS="-I/usr/local/opt/llvm/include"
$ LDFLAGS="-L/usr/local/opt/llvm/lib -Wl,-rpath,/usr/local/opt/llvm/lib"
$ cmake .. -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang -DCMAKE_C_COMPILER=/usr/local/opt/llvm/bin/clang
$ ninja
$ cd ..
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
$ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Reboot.

To check if it’s working, you can use `ulimit -n` command.

[Original article](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->
