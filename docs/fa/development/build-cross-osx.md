---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 66
toc_title: "\u0686\u06AF\u0648\u0646\u0647 \u0628\u0631\u0627\u06CC \u0633\u0627\u062E\
  \u062A \u062A\u0627\u062A\u0631 \u062F\u0631 \u0644\u06CC\u0646\u0648\u06A9\u0633\
  \ \u0628\u0631\u0627\u06CC \u0633\u06CC\u0633\u062A\u0645 \u0639\u0627\u0645\u0644\
  \ \u0645\u06A9 \u0627\u06CC\u06A9\u0633"
---

# چگونه برای ساخت تاتر در لینوکس برای سیستم عامل مک ایکس {#how-to-build-clickhouse-on-linux-for-mac-os-x}

این برای مواردی است که شما دستگاه لینوکس دارید و می خواهید از این برای ساخت استفاده کنید `clickhouse` این است که برای چک ادغام مداوم است که بر روی سرور های لینوکس اجرا در نظر گرفته شده. اگر شما می خواهید برای ساخت خانه کلیک به طور مستقیم در سیستم عامل مک ایکس, سپس با ادامه [دستورالعمل دیگر](build-osx.md).

کراس ساخت برای سیستم عامل مک ایکس بر اساس [ساخت دستورالعمل](build.md) اول دنبالشون کن

# نصب کلانگ-8 {#install-clang-8}

دستورالعمل از دنبال https://apt.llvm.org / برای اوبونتو یا دبیان راه اندازی خود را.
به عنوان مثال دستورات برای بیونیک مانند:

``` bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

# نصب مجموعه ابزار صلیب کشی {#install-cross-compilation-toolset}

بیایید مسیری را که ما نصب می کنیم به یاد داشته باشیم `cctools` به عنوان ${CCTOOLS}

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

همچنین, ما نیاز به دانلود ماکو ایکس انحراف معیار به درخت کار.

``` bash
cd ClickHouse
wget 'https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz'
mkdir -p build-darwin/cmake/toolchain/darwin-x86_64
tar xJf MacOSX10.14.sdk.tar.xz -C build-darwin/cmake/toolchain/darwin-x86_64 --strip-components=1
```

# ساخت خانه کلیک {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-osx
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-osx -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake \
    -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar \
    -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib \
    -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld
ninja -C build-osx
```

باینری حاصل یک فرمت اجرایی ماخ ای داشته باشد و نمی تواند در لینوکس اجرا شود.
