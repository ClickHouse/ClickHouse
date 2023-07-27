---
slug: /en/development/build
sidebar_position: 64
sidebar_label: Build on Linux
title: How to Build ClickHouse on Linux
description: How to build ClickHouse on Linux
---


Supported platforms:

- x86_64
- AArch64
- PowerPC 64 LE (experimental)
- RISC-V 64 (experimental)

## Building on Ubuntu

The following tutorial is based on Ubuntu Linux.
With appropriate changes, it should also work on any other Linux distribution.
The minimum recommended Ubuntu version for development is 22.04 LTS.

### Install Prerequisites {#install-prerequisites}

``` bash
sudo apt-get install git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg
```

### Install and Use the Clang compiler

On Ubuntu/Debian you can use LLVM's automatic installation script, see [here](https://apt.llvm.org/).

``` bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

Note: in case of troubles, you can also use this:

```bash
sudo apt-get install software-properties-common
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
```

For other Linux distribution - check the availability of LLVM's [prebuild packages](https://releases.llvm.org/download.html).

As of April 2023, clang-16 or higher will work.
GCC as a compiler is not supported.
To build with a specific Clang version:

:::tip
This is optional, if you are following along and just now installed Clang then check
to see what version you have installed before setting this environment variable.
:::

``` bash
export CC=clang-16
export CXX=clang++-16
```

### Checkout ClickHouse Sources {#checkout-clickhouse-sources}

``` bash
git clone --recursive --shallow-submodules git@github.com:ClickHouse/ClickHouse.git
```

or

``` bash
git clone --recursive --shallow-submodules https://github.com/ClickHouse/ClickHouse.git
```

### Build ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build
cmake -S . -B build
cmake --build build  # or: `cd build; ninja`
```

To create an executable, run `cmake --build build --target clickhouse` (or: `cd build; ninja clickhouse`).
This will create executable `build/programs/clickhouse` which can be used with `client` or `server` arguments.

## Building on Any Linux {#how-to-build-clickhouse-on-any-linux}

The build requires the following components:

- Git (used to checkout the sources, not needed for the build)
- CMake 3.20 or newer
- Compiler: clang-16 or newer
- Linker: lld-16 or newer
- Ninja
- Yasm
- Gawk

If all the components are installed, you may build in the same way as the steps above.

Example for OpenSUSE Tumbleweed:

``` bash
sudo zypper install git cmake ninja clang-c++ python lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

Example for Fedora Rawhide:

``` bash
sudo yum update
sudo yum --nogpg install git cmake make clang python3 ccache nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```
