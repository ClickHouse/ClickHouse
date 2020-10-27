---
toc_priority: 64
toc_title: Build on Linux
---

# How to Build ClickHouse on Linux {#how-to-build-clickhouse-for-development}

Supported platforms:

-   x86\_64
-   AArch64
-   Power9 (experimental)

## Normal Build for Development on Ubuntu

The following tutorial is based on the Ubuntu Linux system. With appropriate changes, it should also work on any other Linux distribution.

### Install Git, CMake, Python and Ninja {#install-git-cmake-python-and-ninja}

``` bash
$ sudo apt-get install git cmake python ninja-build
```

Or cmake3 instead of cmake on older systems.

### Install GCC 9 {#install-gcc-9}

There are several ways to do this.

#### Install from Repository {#install-from-repository}

On Ubuntu 19.10 or newer:

    $ sudo apt-get update
    $ sudo apt-get install gcc-9 g++-9

#### Install from a PPA Package {#install-from-a-ppa-package}

On older Ubuntu:

``` bash
$ sudo apt-get install software-properties-common
$ sudo apt-add-repository ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install gcc-9 g++-9
```

#### Install from Sources {#install-from-sources}

See [utils/ci/build-gcc-from-sources.sh](https://github.com/ClickHouse/ClickHouse/blob/master/utils/ci/build-gcc-from-sources.sh)

### Use GCC 9 for Builds {#use-gcc-9-for-builds}

``` bash
$ export CC=gcc-9
$ export CXX=g++-9
```

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
-   Ninja (recommended) or Make
-   C++ compiler: gcc 9 or clang 8 or newer
-   Linker: lld or gold (the classic GNU ld won’t work)
-   Python (is only used inside LLVM build and it is optional)

If all the components are installed, you may build in the same way as the steps above.

Example for Ubuntu Eoan:
``` bash
sudo apt update
sudo apt install git cmake ninja-build g++ python
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build && cd build
cmake ../ClickHouse
ninja
```

Example for OpenSUSE Tumbleweed:
``` bash
sudo zypper install git cmake ninja gcc-c++ python lld
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build && cd build
cmake ../ClickHouse
ninja
```

Example for Fedora Rawhide:
``` bash
sudo yum update
yum --nogpg install git cmake make gcc-c++ python2
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build && cd build
cmake ../ClickHouse
make -j $(nproc)
```


## How to Build ClickHouse Debian Package {#how-to-build-clickhouse-debian-package}

### Install Git and Pbuilder {#install-git-and-pbuilder}

``` bash
$ sudo apt-get update
$ sudo apt-get install git python pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
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

## You Don’t Have to Build ClickHouse {#you-dont-have-to-build-clickhouse}

ClickHouse is available in pre-built binaries and packages. Binaries are portable and can be run on any Linux flavour.

They are built for stable, prestable and testing releases as long as for every commit to master and for every pull request.

To find the freshest build from `master`, go to [commits page](https://github.com/ClickHouse/ClickHouse/commits/master), click on the first green checkmark or red cross near commit, and click to the “Details” link right after “ClickHouse Build Check”.

[Original article](https://clickhouse.tech/docs/en/development/build/) <!--hide-->
