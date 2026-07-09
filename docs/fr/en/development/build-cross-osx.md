---
description: 'Guide de compilation croisée de ClickHouse depuis Linux pour les systèmes macOS'
sidebar_label: 'Compiler sous Linux pour macOS'
sidebar_position: 20
slug: /development/build-cross-osx
title: 'Compiler sous Linux pour macOS'
doc_type: 'guide'
---

Ce guide s&#39;adresse au cas où vous disposez d&#39;une machine Linux et souhaitez l&#39;utiliser pour compiler le binaire `clickhouse` qui s&#39;exécutera sur OS X.
Le principal cas d&#39;usage concerne les vérifications d&#39;intégration continue exécutées sur des machines Linux.
Si vous souhaitez compiler ClickHouse directement sur macOS, suivez les [instructions de compilation native](../development/build-osx.md).

La compilation croisée pour macOS repose sur les [instructions de compilation](../development/build.md) ; suivez-les d&#39;abord.

Les sections suivantes présentent les étapes de compilation de ClickHouse pour macOS en `x86_64`.
Si vous ciblez l&#39;architecture ARM, remplacez simplement toutes les occurrences de `x86_64` par `aarch64`.
Par exemple, remplacez `x86_64-apple-darwin` par `aarch64-apple-darwin` dans l&#39;ensemble des étapes.

<div id="install-cross-compilation-toolset">
  ## Installer l’ensemble d’outils de compilation croisée
</div>

Retenons le chemin où nous installons `cctools`, soit `${CCTOOLS}`

```bash
mkdir ~/cctools
export CCTOOLS=$(cd ~/cctools && pwd)
cd ${CCTOOLS}

git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
git checkout 15dfc2a8c9a2a89d06ff227560a69f5265b692f9
INSTALLPREFIX=${CCTOOLS} ./build.sh
./install.sh
cd ..

git clone https://github.com/tpoechtrager/cctools-port.git
cd cctools-port/cctools
git checkout 2a3e1c2a6ff54a30f898b70cfb9ba1692a55fad7
./configure --prefix=$(readlink -f ${CCTOOLS}) --with-libtapi=$(readlink -f ${CCTOOLS}) --target=x86_64-apple-darwin
make install
```

Par ailleurs, nous devons télécharger le SDK macOS X dans le répertoire de travail.

```bash
cd ClickHouse/cmake/toolchain/darwin-x86_64
curl -L 'https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz' | tar xJ --strip-components=1
```

<div id="build-clickhouse">
  ## Compiler ClickHouse
</div>

```bash
cd ClickHouse
mkdir build-darwin
cd build-darwin
cmake -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=${CCTOOLS}/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake ..
ninja
```

Le binaire obtenu sera au format exécutable Mach-O et ne pourra pas être exécuté sous Linux.