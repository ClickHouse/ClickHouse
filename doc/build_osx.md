# How to build ClickHouse

Build should work on Mac OS X 10.12. If you're using earlier version, you can try to build ClickHouse using Gentoo Prefix and clang sl in this instruction.
With appropriate changes, build should work on any other OS X distribution.

## Install Homebrew

```
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## Install cmake, gcc

```
brew install cmake gcc
```

## Install GCC-compatible version of boost

```
brew install boost --cc=gcc-6
```

## Install required libraries

```
brew install icu4c mysql openssl unixodbc libtool gettext homebrew/dupes/libiconv homebrew/dupes/zlib
```

## Install optional libraries

```
brew install readline
```

# Checkout ClickHouse sources

To get the latest stable version:

```
git clone -b stable git@github.com:yandex/ClickHouse.git
# or: git clone -b stable https://github.com/yandex/ClickHouse.git

cd ClickHouse
```

For development, switch to the `master` branch.
For the latest release candidate, switch to the `testing` branch.


## Use GCC 6 for builds

```
export CC=gcc-6
export CXX=g++-6
```

## Disable MongoDB binding
```
export ENABLE_MONGODB=0
```

## Detect number of threads

```
export THREADS=$(sysctl -n hw.ncpu)
```

# Build ClickHouse

```
mkdir build
cd build
cmake ..
make -j $THREADS
cd ..
```

# Caveats

If you intend to run clickhouse-server, make sure to increase system's maxfiles variable. See [MacOS.md](https://github.com/yandex/ClickHouse/blob/master/MacOS.md) for more details.
