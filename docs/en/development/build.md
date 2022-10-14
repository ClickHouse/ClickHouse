---
sidebar_position: 64
sidebar_label: Build on Linux
description: How to build ClickHouse on Linux
---

# How to Build ClickHouse on Linux

Supported platforms:

-   x86_64
-   AArch64
-   Power9 (experimental)

## Normal Build for Development on Ubuntu

The following tutorial is based on the Ubuntu Linux system. With appropriate changes, it should also work on any other Linux distribution.

### Install Git, CMake, Python and Ninja {#install-git-cmake-python-and-ninja}

``` bash
sudo apt-get install git cmake ccache python3 ninja-build
```

Or cmake3 instead of cmake on older systems.

### Install the latest clang (recommended)

On Ubuntu/Debian you can use the automatic installation script (check [official webpage](https://apt.llvm.org/))

```bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

For other Linux distribution - check the availability of the [prebuild packages](https://releases.llvm.org/download.html) or build clang [from sources](https://clang.llvm.org/get_started.html).

#### Use the latest clang for Builds

``` bash
export CC=clang-14
export CXX=clang++-14
```

In this example we use version 14 that is the latest as of Feb 2022.

Gcc can also be used though it is discouraged.

### Checkout ClickHouse Sources {#checkout-clickhouse-sources}

``` bash
git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

or

``` bash
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
```

### Build ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build
cd build
cmake ..
ninja
```

To create an executable, run `ninja clickhouse`.
This will create the `programs/clickhouse` executable, which can be used with `client` or `server` arguments.

## How to Build ClickHouse on Any Linux {#how-to-build-clickhouse-on-any-linux}

The build requires the following components:

-   Git (is used only to checkout the sources, it’s not needed for the build)
-   CMake 3.15 or newer
-   Ninja
-   C++ compiler: clang-14 or newer
-   Linker: lld

If all the components are installed, you may build in the same way as the steps above.

Example for Ubuntu Eoan:
``` bash
sudo apt update
sudo apt install git cmake ninja-build clang++ python
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build && cd build
cmake ../ClickHouse
ninja
```

Example for OpenSUSE Tumbleweed:
``` bash
sudo zypper install git cmake ninja clang-c++ python lld
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build && cd build
cmake ../ClickHouse
ninja
```

Example for Fedora Rawhide:
``` bash
sudo yum update
yum --nogpg install git cmake make clang-c++ python3
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build && cd build
cmake ../ClickHouse
make -j $(nproc)
```

Here is an example of how to build `clang` and all the llvm infrastructure from sources:

```
git clone git@github.com:llvm/llvm-project.git
mkdir llvm-build && cd llvm-build
cmake -DCMAKE_BUILD_TYPE:STRING=Release -DLLVM_ENABLE_PROJECTS=all ../llvm-project/llvm/
make -j16
sudo make install
hash clang
clang --version
```

You can install the older clang like clang-11 from packages and then use it to build the new clang from sources.

Here is an example of how to install the new `cmake` from the official website:

```
wget https://github.com/Kitware/CMake/releases/download/v3.22.2/cmake-3.22.2-linux-x86_64.sh
chmod +x cmake-3.22.2-linux-x86_64.sh
./cmake-3.22.2-linux-x86_64.sh
export PATH=/home/milovidov/work/cmake-3.22.2-linux-x86_64/bin/:${PATH}
hash cmake
```

## How to Build ClickHouse Debian Package {#how-to-build-clickhouse-debian-package}

### Install Git {#install-git}

``` bash
sudo apt-get update
sudo apt-get install git python debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

### Checkout ClickHouse Sources {#checkout-clickhouse-sources-1}

``` bash
git clone --recursive --branch master https://github.com/ClickHouse/ClickHouse.git
cd ClickHouse
```

### Run Release Script {#run-release-script}

``` bash
./release
```

## You Don’t Have to Build ClickHouse {#you-dont-have-to-build-clickhouse}

ClickHouse is available in pre-built binaries and packages. Binaries are portable and can be run on any Linux flavour.

They are built for stable, prestable and testing releases as long as for every commit to master and for every pull request.

To find the freshest build from `master`, go to [commits page](https://github.com/ClickHouse/ClickHouse/commits/master), click on the first green check mark or red cross near commit, and click to the “Details” link right after “ClickHouse Build Check”.
