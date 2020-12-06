---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 64
toc_title: "Linux \xFCzerinde ClickHouse nas\u0131l olu\u015Fturulur"
---

# Geliştirme için ClickHouse nasıl inşa edilir {#how-to-build-clickhouse-for-development}

Aşağıdaki öğretici Ubuntu Linux sistemine dayanmaktadır.
Uygun değişikliklerle, başka herhangi bir Linux dağıtımı üzerinde de çalışması gerekir.
Desteklenen platformlar: x86_64 ve AArch64. Power9 için destek deneyseldir.

## Git, Cmake, Python ve Ninja'yı yükleyin {#install-git-cmake-python-and-ninja}

``` bash
$ sudo apt-get install git cmake python ninja-build
```

Veya eski sistemlerde cmake yerine cmake3.

## Gcc 10'u yükle {#install-gcc-10}

Bunu yapmak için çeşitli yollar vardır.

### Bir PPA paketinden yükleme {#install-from-a-ppa-package}

``` bash
$ sudo apt-get install software-properties-common
$ sudo apt-add-repository ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install gcc-10 g++-10
```

### Kaynaklardan yükleyin {#install-from-sources}

Bakmak [utils/ci/build-gcc-from-sources.sh](https://github.com/ClickHouse/ClickHouse/blob/master/utils/ci/build-gcc-from-sources.sh)

## Yapılar için GCC 10 kullanın {#use-gcc-10-for-builds}

``` bash
$ export CC=gcc-10
$ export CXX=g++-10
```

## Checkout ClickHouse Kaynakları {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

veya

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git
```

## ClickHouse İnşa {#build-clickhouse}

``` bash
$ cd ClickHouse
$ mkdir build
$ cd build
$ cmake ..
$ ninja
$ cd ..
```

Bir yürütülebilir dosya oluşturmak için çalıştırın `ninja clickhouse`.
Bu yaratacak `programs/clickhouse` ile kullanılabilecek çalıştırılabilir `client` veya `server` değişkenler.

# Herhangi bir Linux üzerinde ClickHouse nasıl oluşturulur {#how-to-build-clickhouse-on-any-linux}

Yapı aşağıdaki bileşenleri gerektirir:

-   Git (yalnızca kaynakları kontrol etmek için kullanılır, yapı için gerekli değildir)
-   Cmake 3.10 veya daha yeni
-   Ninja (önerilir) veya yapmak
-   C ++ derleyici: gcc 10 veya clang 8 veya daha yeni
-   Linker :lld veya altın (klasik GNU ld çalışmaz)
-   Python (sadece LLVM yapısında kullanılır ve isteğe bağlıdır)

Tüm bileşenler yüklüyse, yukarıdaki adımlarla aynı şekilde oluşturabilirsiniz.

Ubuntu Eoan için örnek:

    sudo apt update
    sudo apt install git cmake ninja-build g++ python
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

OpenSUSE Tumbleweed için örnek:

    sudo zypper install git cmake ninja gcc-c++ python lld
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

Fedora Rawhide için örnek:

    sudo yum update
    yum --nogpg install git cmake make gcc-c++ python3
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    make -j $(nproc)

# ClickHouse inşa etmek zorunda değilsiniz {#you-dont-have-to-build-clickhouse}

ClickHouse önceden oluşturulmuş ikili ve paketlerde mevcuttur. İkili dosyalar taşınabilir ve herhangi bir Linux lezzet üzerinde çalıştırılabilir.

Onlar sürece her Master taahhüt ve her çekme isteği için kararlı, prestable ve test bültenleri için inşa edilmiştir.

En taze yapıyı bulmak için `master`, go to [taahhüt sayfası](https://github.com/ClickHouse/ClickHouse/commits/master), commit yakınındaki ilk yeşil onay işaretini veya kırmızı çarpı işaretini tıklayın ve “Details” hemen sonra bağlantı “ClickHouse Build Check”.

# ClickHouse Debian paketi nasıl oluşturulur {#how-to-build-clickhouse-debian-package}

## Git ve Pbuilder'ı yükleyin {#install-git-and-pbuilder}

``` bash
$ sudo apt-get update
$ sudo apt-get install git python pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## Checkout ClickHouse Kaynakları {#checkout-clickhouse-sources-1}

``` bash
$ git clone --recursive --branch master https://github.com/ClickHouse/ClickHouse.git
$ cd ClickHouse
```

## Run Release Script {#run-release-script}

``` bash
$ ./release
```

[Orijinal makale](https://clickhouse.tech/docs/en/development/build/) <!--hide-->
