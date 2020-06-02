---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 67
toc_title: "C\xF3mo construir ClickHouse en Linux para AARCH64 (ARM64)"
---

# Cómo construir ClickHouse en Linux para la arquitectura AARCH64 (ARM64) {#how-to-build-clickhouse-on-linux-for-aarch64-arm64-architecture}

Esto es para el caso cuando tiene una máquina Linux y desea usarla para compilar `clickhouse` binario que se ejecutará en otra máquina Linux con arquitectura de CPU AARCH64. Esto está destinado a las comprobaciones de integración continua que se ejecutan en servidores Linux.

La compilación cruzada para AARCH64 se basa en el [Instrucciones de construcción](build.md), seguirlos primero.

# Instalar Clang-8 {#install-clang-8}

Siga las instrucciones de https://apt.llvm.org/ para la configuración de Ubuntu o Debian.
Por ejemplo, en Ubuntu Bionic puede usar los siguientes comandos:

``` bash
echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" | sudo tee /etc/apt/sources.list.d/llvm.list
sudo apt-get update
sudo apt-get install clang-8
```

# Instalar conjunto de herramientas de compilación cruzada {#install-cross-compilation-toolset}

``` bash
cd ClickHouse
mkdir -p build-aarch64/cmake/toolchain/linux-aarch64
wget 'https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en' -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
tar xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz -C build-aarch64/cmake/toolchain/linux-aarch64 --strip-components=1
```

# Construir ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-arm64
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-arm64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake
ninja -C build-arm64
```

El binario resultante se ejecutará solo en Linux con la arquitectura de CPU AARCH64.
