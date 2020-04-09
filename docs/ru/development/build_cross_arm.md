---
machine_translated: true
machine_translated_rev: 1cd5f0028d917696daf71ac1c9ee849c99c1d5c8
---

# Как построить ClickHouse на Linux для архитектуры AArch64 (ARM64)  {#how-to-build-clickhouse-on-linux-for-aarch64-arm64-architecture}

Это для случая, когда у вас есть Linux-машина и вы хотите использовать ее для сборки `clickhouse` двоичный файл, который будет работать на другой машине Linux с архитектурой процессора AARCH64. Это предназначено для непрерывной проверки интеграции, которая выполняется на серверах Linux.

Кросс-сборка для AARCH64 основана на следующих принципах: [Инструкции по сборке](build.md)- сначала следуйте за ними.

# Установка Clang-8 {#install-clang-8}

Следуйте инструкциям от https://apt.llvm.org/ для вашей установки Ubuntu или Debian.
Например, в Ubuntu Bionic вы можете использовать следующие команды:

``` bash
echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" | sudo tee /etc/apt/sources.list.d/llvm.list
sudo apt-get update
sudo apt-get install clang-8
```

# Установка Набора Инструментов Перекрестной Компиляции {#install-cross-compilation-toolset}

``` bash
cd ClickHouse
mkdir -p build-aarch64/cmake/toolchain/linux-aarch64
wget 'https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en' -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
tar xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz -C build-aarch64/cmake/toolchain/linux-aarch64 --strip-components=1
```

# Построить ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-arm64
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-arm64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake
ninja -C build-arm64
```

Полученный двоичный файл будет работать только в Linux с архитектурой процессора AARCH64.
