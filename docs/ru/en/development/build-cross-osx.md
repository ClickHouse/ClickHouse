---
description: 'Руководство по кросс-компиляции ClickHouse в Linux для систем macOS'
sidebar_label: 'Сборка в Linux для macOS'
sidebar_position: 20
slug: /development/build-cross-osx
title: 'Сборка в Linux для macOS'
doc_type: 'guide'
---

Этот раздел предназначен для случаев, когда у вас есть машина под управлением Linux и вы хотите использовать её для сборки бинарного файла `clickhouse`, который будет работать в OS X.
Основной сценарий использования — проверки CI, выполняемые на машинах Linux.
Если вы хотите собрать ClickHouse непосредственно в macOS, перейдите к [инструкциям по нативной сборке](../development/build-osx.md).

Кросс-сборка для macOS основана на [инструкциях по сборке](../development/build.md), поэтому сначала выполните их.

В следующих разделах приведено пошаговое руководство по сборке ClickHouse для macOS `x86_64`.
Если вам нужна архитектура ARM, просто замените все вхождения `x86_64` на `aarch64`.
Например, замените `x86_64-apple-darwin` на `aarch64-apple-darwin` во всех шагах.

<div id="install-cross-compilation-toolset">
  ## Установите набор инструментов для кросс-компиляции
</div>

Запомним путь, по которому мы устанавливаем `cctools`, как `${CCTOOLS}`

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

Кроме того, нам нужно скачать macOS X SDK в рабочий каталог.

```bash
cd ClickHouse/cmake/toolchain/darwin-x86_64
curl -L 'https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz' | tar xJ --strip-components=1
```

<div id="build-clickhouse">
  ## Соберите ClickHouse
</div>

```bash
cd ClickHouse
mkdir build-darwin
cd build-darwin
cmake -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=${CCTOOLS}/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake ..
ninja
```

Полученный бинарный файл будет в формате исполняемого файла Mach-O и не сможет запускаться в Linux.