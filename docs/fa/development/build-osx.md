---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 65
toc_title: "\u0686\u06AF\u0648\u0646\u0647 \u0628\u0631\u0627\u06CC \u0633\u0627\u062E\
  \u062A \u062A\u0627\u062A\u0631 \u062F\u0631 \u0633\u06CC\u0633\u062A\u0645 \u0639\
  \u0627\u0645\u0644 \u0645\u06A9 \u0627\u06CC\u06A9\u0633"
---

# چگونه برای ساخت تاتر در سیستم عامل مک ایکس {#how-to-build-clickhouse-on-mac-os-x}

ساخت باید در سیستم عامل مک ایکس کار 10.15 (کاتالینا)

## نصب گشتن {#install-homebrew}

``` bash
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## نصب کامپایلرهای مورد نیاز, ابزار, و کتابخانه {#install-required-compilers-tools-and-libraries}

``` bash
$ brew install cmake ninja libtool gettext
```

## پرداخت منابع کلیک {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

یا

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git

$ cd ClickHouse
```

## ساخت خانه کلیک {#build-clickhouse}

``` bash
$ mkdir build
$ cd build
$ cmake .. -DCMAKE_CXX_COMPILER=`which clang++` -DCMAKE_C_COMPILER=`which clang`
$ ninja
$ cd ..
```

## هشدارها {#caveats}

اگر شما قصد اجرای clickhouse-سرور مطمئن شوید که برای افزایش سیستم maxfiles متغیر است.

!!! info "یادداشت"
    باید از سودو استفاده کنی

برای انجام این کار فایل زیر را ایجاد کنید:

/Library/LaunchDaemons/محدود می کند.مکسفیلزجان کلام:

``` xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
        "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>limit.maxfiles</string>
    <key>ProgramArguments</key>
    <array>
      <string>launchctl</string>
      <string>limit</string>
      <string>maxfiles</string>
      <string>524288</string>
      <string>524288</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>ServiceIPC</key>
    <false/>
  </dict>
</plist>
```

دستور زیر را اجرا کنید:

``` bash
$ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

راه اندازی مجدد.

برای بررسی اگر این کار, شما می توانید استفاده کنید `ulimit -n` فرمان.

[مقاله اصلی](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->
