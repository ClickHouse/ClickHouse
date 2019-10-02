# How to Build ClickHouse on Linux for Mac OS X

The cross-build for Mac OS X is based on the Build instructions, follow them first.

# Install Clang-8

Follow the instructions from https://apt.llvm.org/ for your Ubuntu or Debian setup.
For example the commands for Bionic are like:

```bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

# Install Cross-Compilation Toolset

```bash
mkdir cctools

git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
INSTALLPREFIX=../cctools ./build.sh
./install.sh
cd ..

git clone https://github.com/tpoechtrager/cctools-port.git
cd cctools-port/cctools
./configure --prefix=../cctools --with-libtapi=../cctools --target=x86_64-apple-darwin
make install
cd ..

cd cctools
wget https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz
tar xJf MacOSX10.14.sdk.tar.xz
```

Let's remember the path where we created `cctools` directory as ${CCTOOLS_PARENT}

# Build ClickHouse

```bash
cd ClickHouse
mkdir build-osx
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-osx -DCMAKE_SYSTEM_NAME=Darwin \
    -DCMAKE_AR:FILEPATH=${CCTOOLS_PARENT}/cctools/bin/x86_64-apple-darwin-ar \
    -DCMAKE_RANLIB:FILEPATH=${CCTOOLS_PARENT}/cctools/bin/x86_64-apple-darwin-ranlib \
    -DLINKER_NAME=${CCTOOLS_PARENT}/cctools/bin/x86_64-apple-darwin-ld \
    -DSDK_PATH=${CCTOOLS_PARENT}/cctools/MacOSX10.14.sdk \
    -DUSE_SNAPPY=OFF -DENABLE_SSL=OFF -DENABLE_PROTOBUF=OFF -DENABLE_PARQUET=OFF -DENABLE_READLINE=OFF -DENABLE_ICU=OFF -DENABLE_FASTOPS=OFF
ninja -C build-osx
```

The resulting binary will have Mach-O executable format and can't be run on Linux.
