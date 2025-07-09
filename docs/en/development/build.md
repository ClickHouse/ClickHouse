---
description: 'Step-by-step guide for building ClickHouse from source on Linux systems'
sidebar_label: 'Build on Linux'
sidebar_position: 10
slug: /development/build
title: 'How to Build ClickHouse on Linux'
---

# How to Build ClickHouse on Linux

:::info You don't have to build ClickHouse yourself!
You can install pre-built ClickHouse as described in [Quick Start](https://clickhouse.com/#quick-start).
:::

ClickHouse can be build on the following platforms:

- x86_64
- AArch64
- PowerPC 64 LE (experimental)
- s390/x (experimental)
- RISC-V 64 (experimental)

## Assumptions {#assumptions}

The following tutorial is based on Ubuntu Linux but it should also work on any other Linux distribution with appropriate changes.
The minimum recommended Ubuntu version for development is 24.04 LTS.

The tutorial assumes that you have the ClickHouse repository and all submodules locally checked out.

## Install Prerequisites {#install-prerequisites}

First, see the generic [prerequisites documentation](developer-instruction.md).

ClickHouse uses CMake and Ninja for building.

You can optionally install ccache to let the build reuse already compiled object files.

```bash
sudo apt-get update
sudo apt-get install git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg
```

## Install the Clang compiler {#install-the-clang-compiler}

To install Clang on Ubuntu/Debian, use LLVM's automatic installation script from [here](https://apt.llvm.org/).

```bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

For other Linux distributions, check if you can install any of LLVM's [prebuild packages](https://releases.llvm.org/download.html).

As of March 2025, Clang 19 or higher is required.
GCC or other compilers are not supported.

## Install the Rust compiler (optional) {#install-the-rust-compiler-optional}

:::note
Rust is an optional dependency of ClickHouse.
If Rust is not installed, some features of ClickHouse will be omitted from compilation.
:::

First, follow the steps in the official [Rust documentation](https://www.rust-lang.org/tools/install) to install `rustup`.

As with C++ dependencies, ClickHouse uses vendoring to control exactly what's installed and avoid depending on third party services (like the `crates.io` registry).

Although in release mode any rust modern rustup toolchain version should work with these dependencies, if you plan to enable sanitizers you must use a version that matches the exact same `std` as the one used in CI (for which we vendor the crates):

```bash
rustup toolchain install nightly-2024-12-01
rustup default nightly-2024-12-01
rustup component add rust-src
```
## Build ClickHouse {#build-clickhouse}

We recommend to create a separate directory `build` inside `ClickHouse` which contains all build artifacts:

```sh
mkdir build
cd build
```

You can have several different directories (e.g. `build_release`, `build_debug`, etc.) for different build types.

Optional: If you have multiple compiler versions installed, you can optionally specify the exact compiler to use.

```sh
export CC=clang-19
export CXX=clang++-19
```

For development purposes, debug builds are recommended.
Compared to release builds, they have a lower compiler optimization level (`-O`) which provides a better debugging experience.
Also, internal exceptions of type `LOGICAL_ERROR` crash immediately instead of failing gracefully.

```sh
cmake -D CMAKE_BUILD_TYPE=Debug ..
```

Run ninja to build:

```sh
ninja clickhouse
```

If you like to build all the binaries (utilities and tests), run ninja without parameters:

```sh
ninja
```

You can control the number of parallel build jobs using parameter `-j`:

```sh
ninja -j 1 clickhouse-server clickhouse-client
```

:::tip
CMake provides shortcuts for above commands:

```sh
cmake -S . -B build  # configure build, run from repository top-level directory
cmake --build build  # compile
```
:::

## Running the ClickHouse Executable {#running-the-clickhouse-executable}

After the build completed successfully, you find the executable in `ClickHouse/<build_dir>/programs/`:

The ClickHouse server tries to find a configuration file `config.xml` in the current directory.
You can alternative specify a configuration file on the command-line via `-C`.

To connect to the ClickHouse server with `clickhouse-client`, open another terminal, navigate to `ClickHouse/build/programs/` and run `./clickhouse client`.

If you get `Connection refused` message on macOS or FreeBSD, try specifying host address 127.0.0.1:

```bash
clickhouse client --host 127.0.0.1
```

## Advanced Options {#advanced-options}

### Minimal Build {#minimal-build}

If you don't need functionality provided by third-party libraries, you can speed the build further up:

```sh
cmake -DENABLE_LIBRARIES=OFF
```

In case of problems, you are on your own ...

Rust requires an internet connection. To disable Rust support:

```sh
cmake -DENABLE_RUST=OFF
```

### Running the ClickHouse Executable {#running-the-clickhouse-executable-1}

You can replace the production version of ClickHouse binary installed in your system with the compiled ClickHouse binary.
To do that, install ClickHouse on your machine following the instructions from the official website.
Next, run:

```bash
sudo service clickhouse-server stop
sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
sudo service clickhouse-server start
```

Note that `clickhouse-client`, `clickhouse-server` and others are symlinks to the commonly shared `clickhouse` binary.

You can also run your custom-built ClickHouse binary with the config file from the ClickHouse package installed on your system:

```bash
sudo service clickhouse-server stop
sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

### Building on Any Linux {#building-on-any-linux}

Install prerequisites on OpenSUSE Tumbleweed:

```bash
sudo zypper install git cmake ninja clang-c++ python lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

Install prerequisites on Fedora Rawhide:

```bash
sudo yum update
sudo yum --nogpg install git cmake make clang python3 ccache lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

### Building in docker {#building-in-docker}

You can run any build locally in an environment similar to CI using:

```bash
python -m ci.praktika "BUILD_JOB_NAME"
```
where BUILD_JOB_NAME is the job name as shown in the CI report, e.g., "Build (arm_release)", "Build (amd_debug)"

This command pulls the appropriate Docker image `clickhouse/binary-builder` with all required dependencies,
and runs the build script inside it: `./ci/jobs/build_clickhouse.py`

The build output will be placed in `./ci/tmp/`.

It works on both AMD and ARM architectures and requires no additional dependencies other than Docker.


