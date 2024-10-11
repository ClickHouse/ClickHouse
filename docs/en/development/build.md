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
sudo apt-get update
sudo apt-get install git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg
```

### Install and Use the Clang compiler

On Ubuntu/Debian, you can use LLVM's automatic installation script; see [here](https://apt.llvm.org/).

``` bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

Note: in case of trouble, you can also use this:

```bash
sudo apt-get install software-properties-common
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
```

For other Linux distributions - check the availability of LLVM's [prebuild packages](https://releases.llvm.org/download.html).

As of March 2024, clang-17 or higher will work.
GCC as a compiler is not supported.
To build with a specific Clang version:

:::tip
This is optional, if you are following along and just now installed Clang then check
to see what version you have installed before setting this environment variable.
:::

``` bash
export CC=clang-18
export CXX=clang++-18
```

### Install Rust compiler

First follow the steps in the official [rust documentation](https://www.rust-lang.org/tools/install) to install `rustup`.

As with C++ dependencies, ClickHouse uses vendoring to control exactly what's installed and avoid depending on third
party services (like the `crates.io` registry).

Although in release mode any rust modern rustup toolchain version should work with this dependencies, if you plan to
enable sanitizers you must use a version that matches the exact same `std` as the one used in CI (for which we vendor
the crates):

```bash
rustup toolchain install nightly-2024-04-01
rustup default nightly-2024-04-01
rustup component add rust-src
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

:::tip
In case `cmake` isn't able to detect the number of available logical cores, the build will be done by one thread. To overcome this, you can tweak `cmake` to use a specific number of threads with `-j` flag, for example, `cmake --build build -j 16`. Alternatively, you can generate build files with a specific number of jobs in advance to avoid always setting the flag: `cmake -DPARALLEL_COMPILE_JOBS=16 -S . -B build`, where `16` is the desired number of threads.
:::

To create an executable, run `cmake --build build --target clickhouse` (or: `cd build; ninja clickhouse`).
This will create an executable `build/programs/clickhouse`, which can be used with `client` or `server` arguments.

## Building on Any Linux {#how-to-build-clickhouse-on-any-linux}

The build requires the following components:

- Git (used to checkout the sources, not needed for the build)
- CMake 3.20 or newer
- Compiler: clang-18 or newer
- Linker: lld-17 or newer
- Ninja
- Yasm
- Gawk
- rustc

If all the components are installed, you may build it in the same way as the steps above.

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
sudo yum --nogpg install git cmake make clang python3 ccache lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

## Building in docker
We use the docker image `clickhouse/binary-builder` for our CI builds. It contains everything necessary to build the binary and packages. There is a script `docker/packager/packager` to ease the image usage:

```bash
# define a directory for the output artifacts
output_dir="build_results"
# a simplest build
./docker/packager/packager --package-type=binary --output-dir "$output_dir"
# build debian packages
./docker/packager/packager --package-type=deb --output-dir "$output_dir"
# by default, debian packages use thin LTO, so we can override it to speed up the build
CMAKE_FLAGS='-DENABLE_THINLTO=' ./docker/packager/packager --package-type=deb --output-dir "./$(git rev-parse --show-cdup)/build_results"
```
