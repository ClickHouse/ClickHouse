# How to Build ClickHouse on Linux for AARCH64 (ARM64) architecture

This is for the case when you have Linux machine and want to use it to build `clickhouse` binary that will run on another Linux machine with AARCH64 CPU architecture. This is intended for continuous integration checks that run on Linux servers.

The cross-build for AARCH64 is based on the [Build instructions](build.md), follow them first.

# Install Clang-8

Follow the instructions from https://apt.llvm.org/ for your Ubuntu or Debian setup.
For example, in Ubuntu Bionic you can use the following commands:

```bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

# Install Cross-Compilation Toolset

```bash
cd ClickHouse
cd cmake/toolchain/linux-aarch64
wget 'https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en' -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
tar --strip-components=1 -xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
```

# Build ClickHouse

```bash
cd ClickHouse
mkdir build-arm64
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-arm64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake
ninja -C build-arm64
```

The resulting binary will run only on Linux with the AARCH64 CPU architecture.
