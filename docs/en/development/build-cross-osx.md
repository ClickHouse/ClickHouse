---
sidebar_position: 66
sidebar_label: Build on Linux for Mac OS X
---

# How to Build ClickHouse on Linux for Mac OS X 

This is for the case when you have a Linux machine and want to use it to build `clickhouse` binary that will run on OS X. 
This is intended for continuous integration checks that run on Linux servers. If you want to build ClickHouse directly on Mac OS X, then proceed with [another instruction](../development/build-osx.md).

The cross-build for Mac OS X is based on the [Build instructions](../development/build.md), follow them first.

## Install Clang-14

Follow the instructions from https://apt.llvm.org/ for your Ubuntu or Debian setup.
For example the commands for Bionic are like:

``` bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-14 main" >> /etc/apt/sources.list
sudo apt-get install clang-14
```

## Install Cross-Compilation Toolset {#install-cross-compilation-toolset}

Let’s remember the path where we install `cctools` as ${CCTOOLS}

``` bash
export CCTOOLS=$(cd ~/cctools && pwd)
mkdir ${CCTOOLS}
cd ${CCTOOLS}

git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
INSTALLPREFIX=${CCTOOLS} ./build.sh
./install.sh
cd ..

git clone https://github.com/tpoechtrager/cctools-port.git
cd cctools-port/cctools
./configure --prefix=$(readlink -f ${CCTOOLS}) --with-libtapi=$(readlink -f ${CCTOOLS}) --target=x86_64-apple-darwin
make install
```

Also, we need to download macOS X SDK into the working tree.

``` bash
cd ClickHouse/cmake/toolchain/darwin-x86_64
curl -L 'https://github.com/phracker/MacOSX-SDKs/releases/download/10.15/MacOSX10.15.sdk.tar.xz' | tar xJ --strip-components=1
```

## Build ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-darwin
cd build-darwin
CC=clang-14 CXX=clang++-14 cmake -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=${CCTOOLS}/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake ..
ninja
```

The resulting binary will have a Mach-O executable format and can’t be run on Linux.
