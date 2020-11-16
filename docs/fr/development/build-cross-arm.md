---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 67
toc_title: Comment Construire ClickHouse sur Linux pour AARCH64 (ARM64)
---

# Comment Construire ClickHouse sur Linux pour L'Architecture AARCH64 (ARM64)  {#how-to-build-clickhouse-on-linux-for-aarch64-arm64-architecture}

C'est pour le cas où vous avez machine Linux et que vous voulez utiliser pour construire `clickhouse` binaire qui fonctionnera sur une autre machine Linux avec une architecture CPU AARCH64. Ceci est destiné aux contrôles d'intégration continus qui s'exécutent sur des serveurs Linux.

La construction croisée pour AARCH64 est basée sur [Instructions de construction](build.md), suivez d'abord.

# Installer Clang-8 {#install-clang-8}

Suivez les instructions de https://apt.llvm.org/ pour votre configuration Ubuntu ou Debian.
Par exemple, dans Ubuntu Bionic vous pouvez utiliser les commandes suivantes:

``` bash
echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" | sudo tee /etc/apt/sources.list.d/llvm.list
sudo apt-get update
sudo apt-get install clang-8
```

# Installer Un Ensemble D'Outils De Compilation Croisée {#install-cross-compilation-toolset}

``` bash
cd ClickHouse
mkdir -p build-aarch64/cmake/toolchain/linux-aarch64
wget 'https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en' -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
tar xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz -C build-aarch64/cmake/toolchain/linux-aarch64 --strip-components=1
```

# Construire ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-arm64
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-arm64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake
ninja -C build-arm64
```

Le binaire résultant s'exécutera uniquement sur Linux avec l'architecture CPU AARCH64.
