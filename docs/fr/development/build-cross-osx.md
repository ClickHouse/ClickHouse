---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 66
toc_title: Comment Construire ClickHouse sur Linux pour Mac OS X
---

# Comment Construire ClickHouse sur Linux pour Mac OS X {#how-to-build-clickhouse-on-linux-for-mac-os-x}

C'est pour le cas où vous avez machine Linux et que vous voulez utiliser pour construire `clickhouse` binaire qui s'exécutera sur OS X. Ceci est destiné aux contrôles d'intégration continus qui s'exécutent sur des serveurs Linux. Si vous voulez construire ClickHouse directement sur Mac OS X, puis procéder à [une autre instruction](build-osx.md).

Le cross-build pour Mac OS X est basé sur le [Instructions de construction](build.md), suivez d'abord.

# Installer Clang-8 {#install-clang-8}

Suivez les instructions de https://apt.llvm.org/ pour votre configuration Ubuntu ou Debian.
Par exemple les commandes pour Bionic sont comme:

``` bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

# Installer Un Ensemble D'Outils De Compilation Croisée {#install-cross-compilation-toolset}

Souvenons nous du chemin où nous installons `cctools` comme ${CCTOOLS}

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

En outre, nous devons télécharger macOS X SDK dans l'arbre de travail.

``` bash
cd ClickHouse
wget 'https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz'
mkdir -p build-darwin/cmake/toolchain/darwin-x86_64
tar xJf MacOSX10.14.sdk.tar.xz -C build-darwin/cmake/toolchain/darwin-x86_64 --strip-components=1
```

# Construire ClickHouse {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-osx
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-osx -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake \
    -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar \
    -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib \
    -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld
ninja -C build-osx
```

Le binaire résultant aura un format exécutable Mach-O et ne pourra pas être exécuté sous Linux.
