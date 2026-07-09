---
description: 'Пошаговое руководство по сборке ClickHouse из исходного кода на системах Linux'
sidebar_label: 'Сборка на Linux'
sidebar_position: 10
slug: /development/build
title: 'Как собрать ClickHouse на Linux'
doc_type: 'guide'
---

:::info Это руководство по сборке предназначено для участников проекта, которые вносят изменения в сам ClickHouse.
Если вы не изменяете исходный код ClickHouse, вы можете установить готовую сборку ClickHouse, как описано в разделе [Быстрый старт](https://clickhouse.com/docs/get-started/quick-start).
:::

ClickHouse можно собрать на следующих платформах:

* x86&#95;64
* AArch64
* PowerPC 64 LE (экспериментальная)
* s390/x (экспериментальная)
* RISC-V 64 (экспериментальная)

<div id="assumptions">
  ## Предварительные условия
</div>

Приведённое ниже руководство основано на Ubuntu Linux, но при соответствующих изменениях должно работать и в любом другом дистрибутиве Linux.
Минимальная рекомендуемая версия Ubuntu для разработки — 24.04 LTS.

В этом руководстве предполагается, что у вас локально клонированы репозиторий ClickHouse и все его подмодули.

<div id="install-prerequisites">
  ## Установите необходимые предварительные условия
</div>

Сначала ознакомьтесь с общей [документацией по предварительным условиям](developer-instruction.md).

ClickHouse использует CMake и Ninja для сборки.

При желании можно установить ccache, чтобы при сборке повторно использовать уже скомпилированные объектные файлы.

```bash
sudo apt-get update
sudo apt-get install build-essential git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg
```

<div id="install-the-clang-compiler">
  ## Установите компилятор Clang
</div>

Чтобы установить Clang в Ubuntu/Debian, воспользуйтесь автоматическим установочным скриптом LLVM [отсюда](https://apt.llvm.org/).

```bash
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 21
```

Для других дистрибутивов Linux проверьте, можно ли установить один из [готовых пакетов LLVM](https://releases.llvm.org/download.html).

По состоянию на февраль 2026 года требуется Clang 21 или новее.
GCC и другие компиляторы не поддерживаются.

<div id="install-the-rust-compiler-optional">
  ## Установите компилятор Rust (необязательно)
</div>

:::note
Rust — необязательная зависимость ClickHouse.
Если Rust не установлен, некоторые возможности ClickHouse не будут включены при компиляции.
:::

Сначала выполните шаги из официальной [документации Rust](https://www.rust-lang.org/tools/install), чтобы установить `rustup`.

Как и в случае с зависимостями C++, ClickHouse использует вендоринг, чтобы точно контролировать состав устанавливаемых компонентов и не зависеть от сторонних сервисов (таких как реестр `crates.io`).

Хотя в режиме `release` с этими зависимостями должна работать любая современная версия toolchain `rustup`, если вы планируете включить санитайзеры, необходимо использовать версию, у которой `std` в точности совпадает с той, что используется в CI (для которой мы вендорим крейты):

```bash
rustup toolchain install nightly-2026-03-22
rustup default nightly-2026-03-22
rustup component add rust-src
```

<div id="build-clickhouse">
  ## Сборка ClickHouse
</div>

Рекомендуем создать внутри `ClickHouse` отдельный каталог `build`, в котором будут находиться все артефакты сборки:

```sh
mkdir build
cd build
```

У вас может быть несколько разных каталогов (например, `build_release`, `build_debug` и т. д.) для разных типов сборки.

Необязательно: если у вас установлено несколько версий компилятора, при необходимости можно указать, какой именно компилятор использовать.

```sh
export CC=clang-21
export CXX=clang++-21
```

Для целей разработки рекомендуется использовать отладочные сборки.
По сравнению с релизными сборками у них ниже уровень оптимизации компилятора (`-O`), что делает отладку более удобной.
Кроме того, внутренние исключения типа `LOGICAL_ERROR` приводят к немедленному аварийному завершению вместо штатной обработки ошибки.

```sh
cmake -D CMAKE_BUILD_TYPE=Debug ..
```

:::note
Если вы хотите использовать отладчик, например gdb, добавьте `-D DEBUG_O_LEVEL="0"` к приведённой выше команде, чтобы отключить все оптимизации компилятора, которые могут мешать gdb просматривать переменные и получать к ним доступ.
:::

Запустите ninja для сборки:

```sh
ninja clickhouse
```

Если вы хотите собрать все бинарные файлы (утилиты и тесты), запустите ninja без параметров:

```sh
ninja
```

Вы можете задать количество параллельных задач сборки с помощью параметра `-j`:

```sh
ninja -j 1 clickhouse
```

:::note
`clickhouse-server`, `clickhouse-client` и другие подобные бинарные файлы — это символические ссылки в каталоге `programs/`, указывающие на исполняемый файл `clickhouse` после завершения сборки.

:::tip
CMake предоставляет сокращённые варианты приведённых выше команд:

```sh
cmake -S . -B build  # configure build, run from repository top-level directory
cmake --build build  # compile
```

:::

<div id="running-the-clickhouse-executable">
  ## Исполняемый файл ClickHouse
</div>

После успешной сборки исполняемый файл находится в `ClickHouse/<build_dir>/programs/`:

ClickHouse server пытается найти файл конфигурации `config.xml` в текущем каталоге.
Также можно указать файл конфигурации в командной строке с помощью `-C`.

Чтобы подключиться к ClickHouse server с помощью `clickhouse-client`, откройте другой терминал, перейдите в `ClickHouse/build/programs/` и выполните `./clickhouse client`.

Если в macOS или FreeBSD появляется сообщение `Connection refused`, попробуйте указать адрес хоста 127.0.0.1:

```bash
clickhouse client --host 127.0.0.1
```

<div id="advanced-options">
  ## Дополнительные параметры
</div>

<div id="minimal-build">
  ### Минимальная сборка
</div>

Если вам не нужна функциональность сторонних библиотек, вы можете ещё больше ускорить сборку:

```sh
cmake -DENABLE_LIBRARIES=OFF
```

Если возникнут проблемы, разбираться придётся самостоятельно ...

Для Rust требуется подключение к интернету. Чтобы отключить поддержку Rust:

```sh
cmake -DENABLE_RUST=OFF
```

<div id="running-the-clickhouse-executable-1">
  ### Исполняемый файл ClickHouse
</div>

Вы можете заменить установленную в системе продакшн-версию бинарного файла ClickHouse на скомпилированный бинарный файл ClickHouse.
Для этого установите ClickHouse на свой компьютер, следуя инструкциям с официального сайта.
Затем выполните:

```bash
sudo service clickhouse-server stop
sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
sudo service clickhouse-server start
```

Обратите внимание, что `clickhouse-client`, `clickhouse-server` и другие — это символические ссылки на общий бинарный файл `clickhouse`.

Вы также можете запустить собственноручно собранный бинарный файл ClickHouse, используя файл конфигурации из пакета ClickHouse, установленного в вашей системе:

```bash
sudo service clickhouse-server stop
sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

<div id="building-on-any-linux">
  ### Сборка в любом дистрибутиве Linux
</div>

Установите необходимые зависимости в OpenSUSE Tumbleweed:

```bash
sudo zypper install git cmake ninja clang-c++ python lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

Установите необходимые пакеты в Fedora Rawhide:

```bash
sudo yum update
sudo yum --nogpg install git cmake make clang python3 ccache lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

<div id="building-in-docker">
  ### Сборка в Docker
</div>

Любую сборку можно выполнить локально в среде, аналогичной CI, с помощью:

```bash
python -m ci.praktika run "BUILD_JOB_NAME"
```

где BUILD&#95;JOB&#95;NAME — это имя задачи, как показано в отчёте CI, например &quot;Build (arm&#95;release)&quot;, &quot;Build (amd&#95;debug)&quot;

Эта команда скачивает соответствующий Docker-образ `clickhouse/binary-builder` со всеми необходимыми зависимостями
и запускает внутри него скрипт сборки: `./ci/jobs/build_clickhouse.py`

Результат сборки будет помещён в `./ci/tmp/`.

Она работает как на архитектурах AMD, так и ARM и не требует никаких дополнительных зависимостей, кроме Python с доступным модулем `requests` и Docker.