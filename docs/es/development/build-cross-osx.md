---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 66
toc_title: "C\xF3mo construir ClickHouse en Linux para Mac OS X"
---

# Cómo construir ClickHouse en Linux para Mac OS X {#how-to-build-clickhouse-on-linux-for-mac-os-x}

Esto es para el caso cuando tiene una máquina Linux y desea usarla para compilar `clickhouse` Esto está destinado a las comprobaciones de integración continuas que se ejecutan en servidores Linux. Si desea crear ClickHouse directamente en Mac OS X, continúe con [otra instrucción](build-osx.md).

La compilación cruzada para Mac OS X se basa en el [Instrucciones de construcción](build.md), seguirlos primero.

# Instalar Clang-8 {#install-clang-8}

Siga las instrucciones de https://apt.llvm.org/ para la configuración de Ubuntu o Debian.
Por ejemplo, los comandos para Bionic son como:

``` bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

# Instalar conjunto de herramientas de compilación cruzada {#install-cross-compilation-toolset}

Recordemos la ruta donde instalamos `cctools` como ${CCTOOLS}

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

Además, necesitamos descargar macOS X SDK en el árbol de trabajo.

``` bash
cd ClickHouse
wget 'https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz'
mkdir -p build-darwin/cmake/toolchain/darwin-x86_64
tar xJf MacOSX10.14.sdk.tar.xz -C build-darwin/cmake/toolchain/darwin-x86_64 --strip-components=1
```

# Construir ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-osx
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-osx -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake \
    -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar \
    -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib \
    -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld
ninja -C build-osx
```

El binario resultante tendrá un formato ejecutable Mach-O y no se puede ejecutar en Linux.
