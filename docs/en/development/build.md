# How to Build ClickHouse Release Package

## Install Git and Pbuilder

```bash
$ sudo apt-get update
$ sudo apt-get install git pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## Checkout ClickHouse Sources

```bash
$ git clone --recursive --branch master https://github.com/ClickHouse/ClickHouse.git
$ cd ClickHouse
```

## Run Release Script

```bash
$ ./release
```

# How to Build ClickHouse for Development

The following tutorial is based on the Ubuntu Linux system.
With appropriate changes, it should also work on any other Linux distribution.
Supported platforms: x86_64 and AArch64. Support for Power9 is experimental.

## Install Git, CMake and Ninja

```bash
$ sudo apt-get install git cmake ninja-build
```

Or cmake3 instead of cmake on older systems.

## Install GCC 9

There are several ways to do this.

### Install from a PPA Package

```bash
$ sudo apt-get install software-properties-common
$ sudo apt-add-repository ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install gcc-9 g++-9
```

### Install from Sources

Look at [utils/ci/build-gcc-from-sources.sh](https://github.com/ClickHouse/ClickHouse/blob/master/utils/ci/build-gcc-from-sources.sh)

## Use GCC 9 for Builds

```bash
$ export CC=gcc-9
$ export CXX=g++-9
```

## Install Required Libraries from Packages

```bash
$ sudo apt-get install libreadline-dev
```

## Checkout ClickHouse Sources

```bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```
or
```bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git
$ cd ClickHouse
```

## Build ClickHouse

```bash
$ mkdir build
$ cd build
$ cmake ..
$ ninja
$ cd ..
```

To create an executable, run `ninja clickhouse`.
This will create the `dbms/programs/clickhouse` executable, which can be used with `client` or `server` arguments.


[Original article](https://clickhouse.yandex/docs/en/development/build/) <!--hide-->
