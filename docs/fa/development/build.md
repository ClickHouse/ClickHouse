---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 64
toc_title: "\u0646\u062D\u0648\u0647 \u0633\u0627\u062E\u062A \u06A9\u0644\u06CC\u06A9\
  \ \u062F\u0631 \u0644\u06CC\u0646\u0648\u06A9\u0633"
---

# چگونه برای ساخت خانه کلیک برای توسعه {#how-to-build-clickhouse-for-development}

راهنمای زیر بر اساس سیستم لینوکس اوبونتو است.
با تغییرات مناسب, همچنین باید بر روی هر توزیع لینوکس دیگر کار.
سیستم عامل های پشتیبانی شده: ایکس86_64 و عاشق64. پشتیبانی از قدرت9 تجربی است.

## شرح متنی (توضیحات سایت) در صفحات شما دیده نمی شود {#install-git-cmake-python-and-ninja}

``` bash
$ sudo apt-get install git cmake python ninja-build
```

یا سیمک 3 به جای کیک در سیستم های قدیمی تر.

## نصب شورای همکاری خلیج فارس 9 {#install-gcc-10}

راه های مختلفی برای انجام این کار وجود دارد.

### نصب از یک بسته پپا {#install-from-a-ppa-package}

``` bash
$ sudo apt-get install software-properties-common
$ sudo apt-add-repository ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install gcc-10 g++-10
```

### نصب از منابع {#install-from-sources}

نگاه کن [utils/ci/build-gcc-from-sources.sh](https://github.com/ClickHouse/ClickHouse/blob/master/utils/ci/build-gcc-from-sources.sh)

## استفاده از شورای همکاری خلیج فارس 10 برای ساخت {#use-gcc-10-for-builds}

``` bash
$ export CC=gcc-10
$ export CXX=g++-10
```

## پرداخت منابع کلیک {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

یا

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git
```

## ساخت خانه کلیک {#build-clickhouse}

``` bash
$ cd ClickHouse
$ mkdir build
$ cd build
$ cmake ..
$ ninja
$ cd ..
```

برای ایجاد یک اجرایی, اجرا `ninja clickhouse`.
این ایجاد خواهد شد `programs/clickhouse` قابل اجرا است که می تواند با استفاده `client` یا `server` بحث کردن.

# چگونه برای ساخت کلیک بر روی هر لینوکس {#how-to-build-clickhouse-on-any-linux}

ساخت نیاز به اجزای زیر دارد:

-   دستگاه گوارش (استفاده می شود تنها به پرداخت منابع مورد نیاز برای ساخت)
-   کیک 3.10 یا جدیدتر
-   نینجا (توصیه می شود) و یا
-   ج ++ کامپایلر: شورای همکاری خلیج فارس 10 یا صدای شیپور 8 یا جدیدتر
-   لینکر: لیلند یا طلا (کلاسیک گنو الدی کار نخواهد کرد)
-   پایتون (فقط در داخل ساخت لورم استفاده می شود و اختیاری است)

اگر تمام اجزای نصب شده, شما ممکن است در همان راه به عنوان مراحل بالا ساخت.

به عنوان مثال برای اوبونتو ایوان:

    sudo apt update
    sudo apt install git cmake ninja-build g++ python
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

به عنوان مثال برای لینوکس تاج خروس:

    sudo zypper install git cmake ninja gcc-c++ python lld
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    ninja

به عنوان مثال برای فدورا پوست دباغی نشده:

    sudo yum update
    yum --nogpg install git cmake make gcc-c++ python3
    git clone --recursive https://github.com/ClickHouse/ClickHouse.git
    mkdir build && cd build
    cmake ../ClickHouse
    make -j $(nproc)

# شما لازم نیست برای ساخت کلیک {#you-dont-have-to-build-clickhouse}

تاتر در فایل های باینری از پیش ساخته شده و بسته های موجود است. فایل های باینری قابل حمل هستند و می تواند بر روی هر عطر و طعم لینوکس اجرا شود.

تا زمانی که برای هر متعهد به کارشناسی کارشناسی ارشد و برای هر درخواست کشش ساخته شده است برای انتشار پایدار و قابل پرست و تست.

برای پیدا کردن تازه ترین ساخت از `master` برو به [مرتکب صفحه](https://github.com/ClickHouse/ClickHouse/commits/master), با کلیک بر روی اولین علامت سبز یا صلیب قرمز در نزدیکی ارتکاب, کلیک کنید و به “Details” پیوند درست بعد از “ClickHouse Build Check”.

# چگونه برای ساخت مخزن دبیان بسته {#how-to-build-clickhouse-debian-package}

## نصب برنامه جی تی و پل ساز {#install-git-and-pbuilder}

``` bash
$ sudo apt-get update
$ sudo apt-get install git python pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## پرداخت منابع کلیک {#checkout-clickhouse-sources-1}

``` bash
$ git clone --recursive --branch master https://github.com/ClickHouse/ClickHouse.git
$ cd ClickHouse
```

## اجرای اسکریپت انتشار {#run-release-script}

``` bash
$ ./release
```

[مقاله اصلی](https://clickhouse.tech/docs/en/development/build/) <!--hide-->
