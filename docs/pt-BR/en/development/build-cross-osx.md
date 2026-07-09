---
description: 'Guia para compilação cruzada do ClickHouse no Linux para sistemas macOS'
sidebar_label: 'Compilar no Linux para macOS'
sidebar_position: 20
slug: /development/build-cross-osx
title: 'Compilar no Linux para macOS'
doc_type: 'guide'
---

Isto se aplica aos casos em que você tem uma máquina Linux e quer usá-la para compilar o binário `clickhouse` que será executado no OS X.
O principal caso de uso são as verificações de integração contínua executadas em máquinas Linux.
Se você quiser compilar o ClickHouse diretamente no macOS, siga as [instruções de compilação nativa](../development/build-osx.md).

A compilação cruzada para macOS é baseada nas [instruções de compilação](../development/build.md), portanto siga-as primeiro.

As seções a seguir apresentam um passo a passo para compilar o ClickHouse para macOS `x86_64`.
Se o destino for a arquitetura ARM, basta substituir todas as ocorrências de `x86_64` por `aarch64`.
Por exemplo, substitua `x86_64-apple-darwin` por `aarch64-apple-darwin` em todas as etapas.

<div id="install-cross-compilation-toolset">
  ## Instale o conjunto de ferramentas de compilação cruzada
</div>

Vamos usar `${CCTOOLS}` para se referir ao caminho em que instalamos `cctools`

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

Além disso, precisamos baixar o SDK do macOS X para o diretório de trabalho.

```bash
cd ClickHouse/cmake/toolchain/darwin-x86_64
curl -L 'https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz' | tar xJ --strip-components=1
```

<div id="build-clickhouse">
  ## Compilação do ClickHouse
</div>

```bash
cd ClickHouse
mkdir build-darwin
cd build-darwin
cmake -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=${CCTOOLS}/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake ..
ninja
```

O binário resultante terá um formato executável Mach-O e não poderá ser executado em Linux.