# How to build ClickHouse on Mac OS X

Build should work on Mac OS X 10.12. If you're using earlier version, you can try to build ClickHouse using Gentoo Prefix and clang sl in this instruction.
With appropriate changes, it should also work on any other Linux distribution.

## Install Homebrew

```bash
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## Install required compilers, tools, and libraries

```bash
brew install cmake gcc icu4c mysql openssl unixodbc libtool gettext zlib readline boost --cc=gcc-7
```

## Checkout ClickHouse sources

To get the latest stable version:

```bash
git clone -b stable --recursive --depth=10 git@github.com:yandex/ClickHouse.git
# or: git clone -b stable --recursive --depth=10 https://github.com/yandex/ClickHouse.git

cd ClickHouse
```

For development, switch to the `master` branch.
For the latest release candidate, switch to the `testing` branch.

## Build ClickHouse

```bash
mkdir build
cd build
cmake .. -DCMAKE_CXX_COMPILER=`which g++-7` -DCMAKE_C_COMPILER=`which gcc-7`
make -j `sysctl -n hw.ncpu`
cd ..
```

## Caveats

If you intend to run clickhouse-server, make sure to increase the system's maxfiles variable. See [MacOS.md](https://github.com/yandex/ClickHouse/blob/master/MacOS.md) for more details.

