---
machine_translated: true
machine_translated_rev: 1cd5f0028d917696daf71ac1c9ee849c99c1d5c8
---

# Как построить ClickHouse на Linux для Mac OS X {#how-to-build-clickhouse-on-linux-for-mac-os-x}

Это для случая, когда у вас есть Linux-машина и вы хотите использовать ее для сборки `clickhouse` двоичный файл, который будет работать на OS X. Это предназначено для непрерывной проверки интеграции, которая выполняется на серверах Linux. Если вы хотите построить ClickHouse непосредственно на Mac OS X, то продолжайте [еще одна инструкция](build_osx.md).

Кросс-сборка для Mac OS X основана на следующих принципах: [Инструкции по сборке](build.md)- сначала следуйте за ними.

# Установка Clang-8 {#install-clang-8}

Следуйте инструкциям от https://apt.llvm.org/ для вашей установки Ubuntu или Debian.
Например команды для Bionic выглядят так:

``` bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

# Установка Набора Инструментов Перекрестной Компиляции {#install-cross-compilation-toolset}

Давайте вспомним путь, по которому мы устанавливаем `cctools` как ${CCTOOLS}

``` bash
mkdir ${CCTOOLS}

git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
INSTALLPREFIX=${CCTOOLS} ./build.sh
./install.sh
cd ..

git clone https://github.com/tpoechtrager/cctools-port.git
cd cctools-port/cctools
./configure --prefix=${CCTOOLS} --with-libtapi=${CCTOOLS} --target=x86_64-apple-darwin
make install
```

Кроме того, нам нужно загрузить MacOS X SDK в рабочее дерево.

``` bash
cd ClickHouse
wget 'https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz'
mkdir -p build-darwin/cmake/toolchain/darwin-x86_64
tar xJf MacOSX10.14.sdk.tar.xz -C build-darwin/cmake/toolchain/darwin-x86_64 --strip-components=1
```

# Построить ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-osx
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-osx -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake \
    -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar \
    -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib \
    -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld
ninja -C build-osx
```

Полученный двоичный файл будет иметь исполняемый формат Mach-O и не может быть запущен в Linux.
