# How to Build ClickHouse Release Package

## Install Git and Pbuilder

```bash
sudo apt-get update
sudo apt-get install git pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## Checkout ClickHouse Sources

```bash
git clone --recursive --branch stable https://github.com/yandex/ClickHouse.git
cd ClickHouse
```

## Run Release Script

```bash
./release
```

# How to Build ClickHouse for Development

Build should work on Ubuntu Linux.
With appropriate changes, it should also work on any other Linux distribution.
The build process is not intended to work on Mac OS X.
Only x86_64 with SSE 4.2 is supported. Support for AArch64 is experimental.

To test for SSE 4.2, do

```bash
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

## Install Git and CMake

```bash
sudo apt-get install git cmake ninja-build
```

Or cmake3 instead of cmake on older systems.

## Install GCC 7

There are several ways to do this.

### Install from a PPA Package

```bash
sudo apt-get install software-properties-common
sudo apt-add-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install gcc-7 g++-7
```

### Install from Sources

Look at [ci/build-gcc-from-sources.sh](https://github.com/yandex/ClickHouse/blob/master/ci/build-gcc-from-sources.sh)

## Use GCC 7 for Builds

```bash
export CC=gcc-7
export CXX=g++-7
```

## Install Required Libraries from Packages

```bash
sudo apt-get install libicu-dev libreadline-dev
```

## Checkout ClickHouse Sources

```bash
git clone --recursive git@github.com:yandex/ClickHouse.git
# or: git clone --recursive https://github.com/yandex/ClickHouse.git

cd ClickHouse
```

For the latest stable version, switch to the `stable` branch.

## Build ClickHouse

```bash
mkdir build
cd build
cmake ..
ninja
cd ..
```

To create an executable, run `ninja clickhouse`.
This will create the `dbms/programs/clickhouse` executable, which can be used with `client` or `server` arguments.

