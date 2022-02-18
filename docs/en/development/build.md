---
toc_priority: 64
toc_title: Build on Linux
---

# How to Build ClickHouse on Linux {#how-to-build-clickhouse-for-development}

Supported platforms:

-   x86_64
-   AArch64
-   Power9 (experimental)

## Normal Build for Development on Ubuntu

The following tutorial is based on the Ubuntu Linux system. With appropriate changes, it should also work on any other Linux distribution.

### Install Git, CMake, Python and Ninja {#install-git-cmake-python-and-ninja}

``` bash
$ sudo apt-get install git cmake python ninja-build
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
$ export CC=clang-14
$ export CXX=clang++-14
```

In this example we use version 14 that is the latest as of Feb 2022.

Gcc can also be used though it is discouraged.

### Checkout ClickHouse Sources {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

or

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git
```

### Build ClickHouse {#build-clickhouse}

``` bash
$ cd ClickHouse
$ mkdir build
$ cd build
$ cmake ..
$ ninja
```

To create an executable, run `ninja clickhouse`.
This will create the `programs/clickhouse` executable, which can be used with `client` or `server` arguments.

## How to Build ClickHouse on Any Linux {#how-to-build-clickhouse-on-any-linux}

The build requires the following components:

-   Git (is used only to checkout the sources, it’s not needed for the build)
-   CMake 3.10 or newer
-   Ninja
-   C++ compiler: clang-13 or newer
-   Linker: lld
-   Python (is only used inside LLVM build and it is optional)

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


## How to Build ClickHouse Debian Package {#how-to-build-clickhouse-debian-package}

### Install Git {#install-git}

``` bash
$ sudo apt-get update
$ sudo apt-get install git python debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

### Checkout ClickHouse Sources {#checkout-clickhouse-sources-1}

``` bash
$ git clone --recursive --branch master https://github.com/ClickHouse/ClickHouse.git
$ cd ClickHouse
```

### Run Release Script {#run-release-script}

``` bash
$ ./release
```

## Faster builds for development

Normally all tools of the ClickHouse bundle, such as `clickhouse-server`, `clickhouse-client` etc., are linked into a single static executable, `clickhouse`. This executable must be re-linked on every change, which might be slow. One common way to improve build time is to use the 'split' build configuration, which builds a separate binary for every tool, and further splits the code into several shared libraries. To enable this tweak, pass the following flags to `cmake`:

```
-DUSE_STATIC_LIBRARIES=0 -DSPLIT_SHARED_LIBRARIES=1 -DCLICKHOUSE_SPLIT_BINARY=1
```

## You Don’t Have to Build ClickHouse {#you-dont-have-to-build-clickhouse}

ClickHouse is available in pre-built binaries and packages. Binaries are portable and can be run on any Linux flavour.

They are built for stable, prestable and testing releases as long as for every commit to master and for every pull request.

To find the freshest build from `master`, go to [commits page](https://github.com/ClickHouse/ClickHouse/commits/master), click on the first green checkmark or red cross near commit, and click to the “Details” link right after “ClickHouse Build Check”.

## Split build configuration {#split-build}

Normally ClickHouse is statically linked into a single static `clickhouse` binary with minimal dependencies. This is convenient for distribution, but it means that on every change the entire binary is linked again, which is slow and may be inconvenient for development. There is an alternative configuration which creates dynamically loaded shared libraries instead, allowing faster incremental builds. To use it, add the following flags to your `cmake` invocation:
```
-DUSE_STATIC_LIBRARIES=0 -DSPLIT_SHARED_LIBRARIES=1 -DCLICKHOUSE_SPLIT_BINARY=1
```

Note that the split build has several drawbacks:
* There is no single `clickhouse` binary, and you have to run `clickhouse-server`, `clickhouse-client`, etc.
* Risk of segfault if you run any of the programs while rebuilding the project.
* You cannot run the integration tests since they only work a single complete binary.
* You can't easily copy the binaries elsewhere. Instead of moving a single binary you'll need to copy all binaries and libraries.

[Original article](https://clickhouse.com/docs/en/development/build/) <!--hide-->
