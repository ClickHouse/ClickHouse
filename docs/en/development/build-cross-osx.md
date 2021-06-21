---
toc_priority: 66
toc_title: Build on Linux for Mac OS X
---

# How to Build ClickHouse on Linux for Mac OS X {#how-to-build-clickhouse-on-linux-for-mac-os-x}

This is for the case when you have Linux machine and want to use it to build `clickhouse` binary that will run on OS X. This is intended for continuous integration checks that run on Linux servers. If you want to build ClickHouse directly on Mac OS X, then proceed with [another instruction](../development/build-osx.md).

The cross-build for Mac OS X is based on the [Build instructions](../development/build.md), follow them first.

## Install Clang-8 {#install-clang-8}

Follow the instructions from https://apt.llvm.org/ for your Ubuntu or Debian setup.
For example the commands for Bionic are like:

``` bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

## Install Cross-Compilation Toolset {#install-cross-compilation-toolset}

Let’s remember the path where we install `cctools` as ${CCTOOLS}

``` bash
mkdir ${CCTOOLS}

git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
INSTALLPREFIX=${CCTOOLS} ./build.sh
./install.sh
cd ..

git clone https://github.com/tpoechtrager/cctools-port.git
cd cctools-port/cctools
./configure --prefix=${CCTOOLS} --with-libtapi=${CCTOOLS} --target=x86_64-apple-darwin
make install
```

Also, we need to download macOS X SDK into the working tree.

``` bash
cd ClickHouse
wget 'https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz'
mkdir -p build-darwin/cmake/toolchain/darwin-x86_64
tar xJf MacOSX10.14.sdk.tar.xz -C build-darwin/cmake/toolchain/darwin-x86_64 --strip-components=1
```

## Build ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-osx
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-osx -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake \
    -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar \
    -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib \
    -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld
ninja -C build-osx
```

The resulting binary will have a Mach-O executable format and can’t be run on Linux.
