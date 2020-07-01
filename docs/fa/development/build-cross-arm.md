---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 67
toc_title: "\u0686\u06AF\u0648\u0646\u0647 \u0628\u0631\u0627\u06CC \u0633\u0627\u062E\
  \u062A ClickHouse \u062F\u0631 \u0644\u06CC\u0646\u0648\u06A9\u0633 \u0628\u0631\
  \u0627\u06CC AARCH64 (ARM64)"
---

# چگونه برای ساخت ClickHouse در لینوکس برای AARCH64 (ARM64) معماری {#how-to-build-clickhouse-on-linux-for-aarch64-arm64-architecture}

این برای مواردی است که شما دستگاه لینوکس دارید و می خواهید از این برای ساخت استفاده کنید `clickhouse` دودویی که در یک ماشین لینوکس دیگر با معماری پردازنده عاشق64 اجرا خواهد شد. این است که برای چک ادغام مداوم است که بر روی سرور های لینوکس اجرا در نظر گرفته شده.

صلیب-ساخت برای AARCH64 است که بر اساس [ساخت دستورالعمل](build.md) اول دنبالشون کن

# نصب کلانگ-8 {#install-clang-8}

دستورالعمل از دنبال https://apt.llvm.org / برای اوبونتو یا دبیان راه اندازی خود را.
مثلا, در اوبونتو بیونیک شما می توانید دستورات زیر استفاده کنید:

``` bash
echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" | sudo tee /etc/apt/sources.list.d/llvm.list
sudo apt-get update
sudo apt-get install clang-8
```

# نصب مجموعه ابزار صلیب کشی {#install-cross-compilation-toolset}

``` bash
cd ClickHouse
mkdir -p build-aarch64/cmake/toolchain/linux-aarch64
wget 'https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en' -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
tar xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz -C build-aarch64/cmake/toolchain/linux-aarch64 --strip-components=1
```

# ساخت خانه کلیک {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-arm64
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-arm64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake
ninja -C build-arm64
```

باینری حاصل تنها در لینوکس با معماری پردازنده اروچ64 اجرا خواهد شد.
