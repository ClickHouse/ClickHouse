# How to build ClickHouse

Build should work on Linux Ubuntu 12.04, 14.04 or newer.
With appropriate changes, build should work on any other Linux distribution.
Build is not intended to work on Mac OS X.
Only x86_64 with SSE 4.2 is supported. Support for AArch64 is experimental.

To test for SSE 4.2, do
```
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

## Install Git and CMake

```
sudo apt-get install git cmake
```

## Detect number of threads
```
export THREADS=$(grep -c ^processor /proc/cpuinfo)
```

## Install GCC 6

There are several ways to do it.

### 1. Install from PPA package.

```
sudo apt-get install software-properties-common
sudo apt-add-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install gcc-6 g++-6
```

### 2. Install GCC 6 from sources.

Example:
```
# Download gcc from https://gcc.gnu.org/mirrors.html
wget ftp://ftp.fu-berlin.de/unix/languages/gcc/releases/gcc-6.2.0/gcc-6.2.0.tar.bz2
tar xf gcc-6.2.0.tar.bz2
cd gcc-6.2.0
./contrib/download_prerequisites
cd ..
mkdir gcc-build
cd gcc-build
../gcc-6.2.0/configure --enable-languages=c,c++
make -j $THREADS
sudo make install
hash gcc g++
gcc --version
sudo ln -s /usr/local/bin/gcc /usr/local/bin/gcc-6
sudo ln -s /usr/local/bin/g++ /usr/local/bin/g++-6
sudo ln -s /usr/local/bin/gcc /usr/local/bin/cc
sudo ln -s /usr/local/bin/g++ /usr/local/bin/c++
# /usr/local/bin/ should be in $PATH
```

## Use GCC 6 for builds

```
export CC=gcc-6
export CXX=g++-6
```

## Install required libraries from packages

```
sudo apt-get install libicu-dev libreadline-dev libmysqlclient-dev libssl-dev unixodbc-dev
```

# Checkout ClickHouse sources

To get latest stable version:
```
git clone -b stable git@github.com:yandex/ClickHouse.git
# or: git clone -b stable https://github.com/yandex/ClickHouse.git

cd ClickHouse
```

For development, switch to the `master` branch.
For latest release candidate, switch to the `testing` branch.

# Build ClickHouse

There are two variants of build.
## 1. Build release package.

### Install prerequisites to build debian packages.
```
sudo apt-get install devscripts dupload fakeroot debhelper
```

### Install recent version of clang.

Clang is embedded into ClickHouse package and used at runtime. Minimum version is 3.8.0. It is optional.

There are two variants:
#### 1. Build clang from sources.
```
cd ..
sudo apt-get install subversion
mkdir llvm
cd llvm
svn co http://llvm.org/svn/llvm-project/llvm/tags/RELEASE_390/final llvm
cd llvm/tools
svn co http://llvm.org/svn/llvm-project/cfe/tags/RELEASE_390/final clang
cd ..
cd projects/
svn co http://llvm.org/svn/llvm-project/compiler-rt/tags/RELEASE_390/final compiler-rt
cd ../..
mkdir build
cd build/
cmake -D CMAKE_BUILD_TYPE:STRING=Release ../llvm
make -j $THREADS
sudo make install
hash clang
```

#### 2. Install from packages.

On Ubuntu 16.04 or newer:
```
sudo apt-get install clang
```

You may also build ClickHouse with clang for development purposes.
For production releases, GCC is used.

### Run release script.
```
rm -f ../clickhouse*.deb
./release --standalone
```

You will find built packages in parent directory.
```
ls -l ../clickhouse*.deb
```

Note that usage of debian packages is not required.
ClickHouse has no runtime dependencies except libc,
 so it could work on almost any Linux.

### Installing just built packages on development server.
```
sudo dpkg -i ../clickhouse*.deb
sudo service clickhouse-server start
```

## 2. Build to work with code.
```
mkdir build
cd build
cmake ..
make -j $THREADS
cd ..
```
