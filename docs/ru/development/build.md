---
machine_translated: true
machine_translated_rev: 1cd5f0028d917696daf71ac1c9ee849c99c1d5c8
---

# Как построить ClickHouse для развития {#how-to-build-clickhouse-for-development}

Следующий учебник основан на системе Ubuntu Linux.
С соответствующими изменениями он также должен работать на любом другом дистрибутиве Linux.
Поддерживаемые платформы: x86\_64 и AArch64. Поддержка Power9 является экспериментальной.

## Установите Git, CMake, Python и Ninja {#install-git-cmake-python-and-ninja}

``` bash
$ sudo apt-get install git cmake python ninja-build
```

Или cmake3 вместо cmake на старых системах.

## Установка GCC 9 {#install-gcc-9}

Есть несколько способов сделать это.

### Установка из PPA пакет {#install-from-a-ppa-package}

``` bash
$ sudo apt-get install software-properties-common
$ sudo apt-add-repository ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install gcc-9 g++-9
```

### Установка из источников {#install-from-sources}

Смотреть на [utils/ci/build-gcc-from-sources.sh](https://github.com/ClickHouse/ClickHouse/blob/master/utils/ci/build-gcc-from-sources.sh)

## Использовать GCC для сборки 9 {#use-gcc-9-for-builds}

``` bash
$ export CC=gcc-9
$ export CXX=g++-9
```

## Проверка Источников ClickHouse {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

или

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git
```

## Построить ClickHouse {#build-clickhouse}

``` bash
$ cd ClickHouse
$ mkdir build
$ cd build
$ cmake ..
$ ninja
$ cd ..
```

Чтобы создать исполняемый файл, выполните команду `ninja clickhouse`.
Это позволит создать `programs/clickhouse` исполняемый файл, который может быть использован с `client` или `server` аргументы.

# Как построить ClickHouse на любом Linux {#how-to-build-clickhouse-on-any-linux}

Для сборки требуются следующие компоненты:

-   Git (используется только для проверки исходных текстов, он не нужен для сборки)
-   CMake 3.10 или новее
-   Ниндзя (рекомендуется) или сделать
-   Компилятор C++: gcc 9 или clang 8 или новее
-   Компоновщик: lld или gold (классический GNU ld не будет работать)
-   Python (используется только внутри сборки LLVM и является необязательным)

Если все компоненты установлены, Вы можете построить их так же, как и описанные выше шаги.

Пример для Ubuntu Eoan:

    sudo apt update
    sudo apt install git cmake ninja-build g++ python
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

Пример для OpenSUSE перекати-поле:

    sudo zypper install git cmake ninja gcc-c++ python lld
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

Пример для сыромятной кожи Fedora:

    sudo yum update
    yum --nogpg install git cmake make gcc-c++ python2
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    make -j $(nproc)

# Вам не нужно строить ClickHouse {#you-dont-have-to-build-clickhouse}

ClickHouse доступен в готовых двоичных файлах и пакетах. Двоичные файлы являются портативными и могут быть запущены на любом вкусе Linux.

Они созданы для стабильных, предустановленных и тестовых релизов до тех пор, пока для каждого коммита к мастеру и для каждого запроса на вытягивание.

Чтобы найти самую свежую сборку из `master`, обратиться [совершает страницы](https://github.com/ClickHouse/ClickHouse/commits/master), нажмите на первую зеленую галочку или красный крестик рядом с фиксацией и нажмите на кнопку «Details» ссылка сразу после этого «ClickHouse Build Check».

# Как создать пакет ClickHouse Debian {#how-to-build-clickhouse-debian-package}

## Установите Git и Pbuilder {#install-git-and-pbuilder}

``` bash
$ sudo apt-get update
$ sudo apt-get install git python pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## Проверка Источников ClickHouse {#checkout-clickhouse-sources-1}

``` bash
$ git clone --recursive --branch master https://github.com/ClickHouse/ClickHouse.git
$ cd ClickHouse
```

## Запустить Сценарий Выпуска {#run-release-script}

``` bash
$ ./release
```

[Оригинальная статья](https://clickhouse.tech/docs/en/development/build/) <!--hide-->
