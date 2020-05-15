---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 64
toc_title: Comment Construire ClickHouse sur Linux
---

# Comment Construire ClickHouse Pour Le développement {#how-to-build-clickhouse-for-development}

Le tutoriel suivant est basé sur le système Linux Ubuntu.
Avec les modifications appropriées, il devrait également fonctionner sur toute autre distribution Linux.
Plates-formes prises en charge: x86\_64 et AArch64. La prise en charge de Power9 est expérimentale.

## Installez Git, CMake, Python Et Ninja {#install-git-cmake-python-and-ninja}

``` bash
$ sudo apt-get install git cmake python ninja-build
```

Ou cmake3 au lieu de cmake sur les systèmes plus anciens.

## Installer GCC 9 {#install-gcc-9}

Il y a plusieurs façons de le faire.

### Installer à Partir d’un Paquet PPA {#install-from-a-ppa-package}

``` bash
$ sudo apt-get install software-properties-common
$ sudo apt-add-repository ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install gcc-9 g++-9
```

### Installer à Partir De Sources {#install-from-sources}

Regarder [utils/ci/build-gcc-from-sources.sh](https://github.com/ClickHouse/ClickHouse/blob/master/utils/ci/build-gcc-from-sources.sh)

## Utilisez GCC 9 Pour Les Builds {#use-gcc-9-for-builds}

``` bash
$ export CC=gcc-9
$ export CXX=g++-9
```

## Commander Clickhouse Sources {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

ou

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git
```

## Construire ClickHouse {#build-clickhouse}

``` bash
$ cd ClickHouse
$ mkdir build
$ cd build
$ cmake ..
$ ninja
$ cd ..
```

Pour créer un exécutable, exécutez `ninja clickhouse`.
Cela va créer de l’ `programs/clickhouse` exécutable, qui peut être utilisé avec `client` ou `server` argument.

# Comment Construire ClickHouse Sur N’importe Quel Linux {#how-to-build-clickhouse-on-any-linux}

La construction nécessite les composants suivants:

-   Git (est utilisé uniquement pour extraire les sources, ce n’est pas nécessaire pour la construction)
-   CMake 3.10 ou plus récent
-   Ninja (recommandé) ou faire
-   Compilateur C++: gcc 9 ou clang 8 ou plus récent
-   Linker: lld ou gold (le classique GNU LD ne fonctionnera pas)
-   Python (est seulement utilisé dans la construction LLVM et il est facultatif)

Si tous les composants sont installés, vous pouvez construire de la même manière que les étapes ci-dessus.

Exemple pour Ubuntu Eoan:

    sudo apt update
    sudo apt install git cmake ninja-build g++ python
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

Exemple Pour openSUSE Tumbleweed:

    sudo zypper install git cmake ninja gcc-c++ python lld
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

Exemple Pour Fedora Rawhide:

    sudo yum update
    yum --nogpg install git cmake make gcc-c++ python2
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    make -j $(nproc)

# Vous N’avez Pas à Construire ClickHouse {#you-dont-have-to-build-clickhouse}

ClickHouse est disponible dans des binaires et des paquets pré-construits. Les binaires sont portables et peuvent être exécutés sur N’importe quelle saveur Linux.

Ils sont conçus pour les versions stables, préconfigurables et de test aussi longtemps que pour chaque commit à master et pour chaque requête d’extraction.

Pour trouver la construction la plus fraîche de `master`, aller à [page commits](https://github.com/ClickHouse/ClickHouse/commits/master), cliquez sur la première coche verte ou Croix Rouge près de commit, et cliquez sur le “Details” lien à droite après “ClickHouse Build Check”.

# Comment Construire Le Paquet ClickHouse Debian {#how-to-build-clickhouse-debian-package}

## Installer Git Et Pbuilder {#install-git-and-pbuilder}

``` bash
$ sudo apt-get update
$ sudo apt-get install git python pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## Commander Clickhouse Sources {#checkout-clickhouse-sources-1}

``` bash
$ git clone --recursive --branch master https://github.com/ClickHouse/ClickHouse.git
$ cd ClickHouse
```

## Exécuter Le Script De Publication {#run-release-script}

``` bash
$ ./release
```

[Article Original](https://clickhouse.tech/docs/en/development/build/) <!--hide-->
