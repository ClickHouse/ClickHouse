# How to Build ClickHouse on Mac OS X

Build should work on Mac OS X 10.12. If you're using earlier version, you can try to build ClickHouse using Gentoo Prefix and clang sl in this instruction.
With appropriate changes, it should also work on any other Linux distribution.

## Install Homebrew

```bash
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## Install Required Compilers, Tools, and Libraries

```bash
brew install cmake ninja gcc icu4c mariadb-connector-c openssl libtool gettext readline
```

## Checkout ClickHouse Sources

```bash
git clone --recursive --depth=10 git@github.com:yandex/ClickHouse.git
# or: git clone --recursive --depth=10 https://github.com/yandex/ClickHouse.git

cd ClickHouse
```

For the latest stable version, switch to the `stable` branch.

## Build ClickHouse

```bash
mkdir build
cd build
cmake .. -DCMAKE_CXX_COMPILER=`which g++-8`-DCMAKE_C_COMPILER=`which gcc-8`
ninja
cd ..
```

## Caveats

If you intend to run clickhouse-server, make sure to increase the system's maxfiles variable.

!!! info "Note"
    You'll need to use sudo.

To do so, create the following file:

/Library/LaunchDaemons/limit.maxfiles.plist:
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

To check if it's working, you can use `ulimit -n` command.

