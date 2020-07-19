---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 65
toc_title: "Mac OS X \xFCzerinde ClickHouse nas\u0131l olu\u015Fturulur"
---

# Mac OS X üzerinde ClickHouse nasıl oluşturulur {#how-to-build-clickhouse-on-mac-os-x}

Build Mac OS X 10.15 (Catalina) üzerinde çalışmalıdır)

## Homebrew Yüklemek {#install-homebrew}

``` bash
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## Gerekli derleyicileri, araçları ve kitaplıkları yükleyin {#install-required-compilers-tools-and-libraries}

``` bash
$ brew install cmake ninja libtool gettext
```

## Checkout ClickHouse Kaynakları {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

veya

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git

$ cd ClickHouse
```

## ClickHouse İnşa {#build-clickhouse}

``` bash
$ mkdir build
$ cd build
$ cmake .. -DCMAKE_CXX_COMPILER=`which clang++` -DCMAKE_C_COMPILER=`which clang`
$ ninja
$ cd ..
```

## Uyarılar {#caveats}

Clickhouse-server çalıştırmak istiyorsanız, sistemin maxfiles değişken artırmak için emin olun.

!!! info "Not"
    Sudo kullanmanız gerekecek.

Bunu yapmak için aşağıdaki dosyayı oluşturun:

/ Kütüphane / LaunchDaemons / sınırı.maxfiles.plist:

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

Aşağıdaki komutu çalıştırın:

``` bash
$ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Başlatmak.

Çalışıp çalışmadığını kontrol etmek için şunları kullanabilirsiniz `ulimit -n` komut.

[Orijinal makale](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->
