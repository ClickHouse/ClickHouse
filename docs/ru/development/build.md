# How to build ClickHouse on Linux

Build should work on Linux Ubuntu 12.04, 14.04 or newer.
With appropriate changes, build should work on any other Linux distribution.
Build is not intended to work on Mac OS X.
Only x86_64 with SSE 4.2 is supported. Support for AArch64 is experimental.

To test for SSE 4.2, do

```bash
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

## Install Git and CMake

```bash
sudo apt-get install git cmake3
```

Or just cmake on newer systems.

## Detect number of threads

```bash
export THREADS=$(grep -c ^processor /proc/cpuinfo)
```

## Install GCC 7

There are several ways to do it.

### Install from PPA package

```bash
sudo apt-get install software-properties-common
sudo apt-add-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install gcc-7 g++-7
```

### Install from sources

Look at [https://github.com/yandex/ClickHouse/blob/master/utils/prepare-environment/install-gcc.sh]

## Use GCC 7 for builds

```bash
export CC=gcc-7
export CXX=g++-7
```

## Install required libraries from packages

```bash
sudo apt-get install libicu-dev libreadline-dev libmysqlclient-dev libssl-dev unixodbc-dev
```

## Checkout ClickHouse sources

To get latest stable version:

```bash
git clone -b stable --recursive git@github.com:yandex/ClickHouse.git
# or: git clone -b stable --recursive https://github.com/yandex/ClickHouse.git

cd ClickHouse
```

For development, switch to the `master` branch.
For latest release candidate, switch to the `testing` branch.

## Build ClickHouse

There are two variants of build.

### Build release package

Install prerequisites to build debian packages.

```bash
sudo apt-get install devscripts dupload fakeroot debhelper
```

Install recent version of clang.

Clang is embedded into ClickHouse package and used at runtime. Minimum version is 5.0. It is optional.

To install clang, look at `utils/prepare-environment/install-clang.sh`

You may also build ClickHouse with clang for development purposes.
For production releases, GCC is used.

Run release script:

```bash
rm -f ../clickhouse*.deb
./release
```

You will find built packages in parent directory:

```bash
ls -l ../clickhouse*.deb
```

Note that usage of debian packages is not required.
ClickHouse has no runtime dependencies except libc, so it could work on almost any Linux.

Installing just built packages on development server:

```bash
sudo dpkg -i ../clickhouse*.deb
sudo service clickhouse-server start
```

### Build to work with code

```bash
mkdir build
cd build
cmake ..
make -j $THREADS
cd ..
```

To create an executable, run `make clickhouse`.
This will create the `dbms/src/Server/clickhouse` executable, which can be used with `client` or `server` arguments.
