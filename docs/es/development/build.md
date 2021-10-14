---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 64
toc_title: "C\xF3mo crear ClickHouse en Linux"
---

# Cómo construir ClickHouse para el desarrollo {#how-to-build-clickhouse-for-development}

El siguiente tutorial se basa en el sistema Ubuntu Linux.
Con los cambios apropiados, también debería funcionar en cualquier otra distribución de Linux.
Plataformas compatibles: x86_64 y AArch64. El soporte para Power9 es experimental.

## Instalar Git, CMake, Python y Ninja {#install-git-cmake-python-and-ninja}

``` bash
$ sudo apt-get install git cmake python ninja-build
```

O cmake3 en lugar de cmake en sistemas más antiguos.

## Instalar GCC 10 {#install-gcc-10}

Hay varias formas de hacer esto.

### Instalar desde un paquete PPA {#install-from-a-ppa-package}

``` bash
$ sudo apt-get install software-properties-common
$ sudo apt-add-repository ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install gcc-10 g++-10
```

### Instalar desde fuentes {#install-from-sources}

Mira [Sistema abierto.](https://github.com/ClickHouse/ClickHouse/blob/master/utils/ci/build-gcc-from-sources.sh)

## Usar GCC 10 para compilaciones {#use-gcc-10-for-builds}

``` bash
$ export CC=gcc-10
$ export CXX=g++-10
```

## Fuentes de ClickHouse de pago {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

o

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git
```

## Construir ClickHouse {#build-clickhouse}

``` bash
$ cd ClickHouse
$ mkdir build
$ cd build
$ cmake ..
$ ninja
$ cd ..
```

Para crear un ejecutable, ejecute `ninja clickhouse`.
Esto creará el `programs/clickhouse` ejecutable, que se puede usar con `client` o `server` argumento.

# Cómo construir ClickHouse en cualquier Linux {#how-to-build-clickhouse-on-any-linux}

La compilación requiere los siguientes componentes:

-   Git (se usa solo para verificar las fuentes, no es necesario para la compilación)
-   CMake 3.10 o más reciente
-   Ninja (recomendado) o Hacer
-   Compilador de C ++: gcc 10 o clang 8 o más reciente
-   Enlazador: lld u oro (el clásico GNU ld no funcionará)
-   Python (solo se usa dentro de la compilación LLVM y es opcional)

Si todos los componentes están instalados, puede compilar de la misma manera que los pasos anteriores.

Ejemplo para Ubuntu Eoan:

    sudo apt update
    sudo apt install git cmake ninja-build g++ python
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

Ejemplo de OpenSUSE Tumbleweed:

    sudo zypper install git cmake ninja gcc-c++ python lld
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

Ejemplo de Fedora Rawhide:

    sudo yum update
    yum --nogpg install git cmake make gcc-c++ python3
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    make -j $(nproc)

# No tienes que construir ClickHouse {#you-dont-have-to-build-clickhouse}

ClickHouse está disponible en binarios y paquetes preconstruidos. Los binarios son portátiles y se pueden ejecutar en cualquier tipo de Linux.

Están diseñados para lanzamientos estables, preestablecidos y de prueba, siempre que para cada compromiso con el maestro y para cada solicitud de extracción.

Para encontrar la construcción más fresca de `master`, ir a [se compromete página](https://github.com/ClickHouse/ClickHouse/commits/master), haga clic en la primera marca de verificación verde o cruz roja cerca de confirmar, y haga clic en “Details” enlace justo después “ClickHouse Build Check”.

# Cómo construir el paquete Debian ClickHouse {#how-to-build-clickhouse-debian-package}

## Instalar Git y Pbuilder {#install-git-and-pbuilder}

``` bash
$ sudo apt-get update
$ sudo apt-get install git python pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## Fuentes de ClickHouse de pago {#checkout-clickhouse-sources-1}

``` bash
$ git clone --recursive --branch master https://github.com/ClickHouse/ClickHouse.git
$ cd ClickHouse
```

## Ejecutar secuencia de comandos de lanzamiento {#run-release-script}

``` bash
$ ./release
```

[Artículo Original](https://clickhouse.tech/docs/en/development/build/) <!--hide-->
