---
machine_translated: true
machine_translated_rev: 1cd5f0028d917696daf71ac1c9ee849c99c1d5c8
---

# Как построить ClickHouse на Mac OS X {#how-to-build-clickhouse-on-mac-os-x}

Сборка должна работать на Mac OS X 10.15 (Catalina)

## Установите Homebrew {#install-homebrew}

``` bash
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## Установите необходимые компиляторы, инструменты и библиотеки {#install-required-compilers-tools-and-libraries}

``` bash
$ brew install cmake ninja libtool gettext
```

## Проверка Источников ClickHouse {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

или

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git

$ cd ClickHouse
```

## Построить ClickHouse {#build-clickhouse}

``` bash
$ mkdir build
$ cd build
$ cmake .. -DCMAKE_CXX_COMPILER=`which clang++` -DCMAKE_C_COMPILER=`which clang`
$ ninja
$ cd ..
```

## Предостережения {#caveats}

Если вы собираетесь запустить clickhouse-сервер, убедитесь в том, чтобы увеличить параметром maxfiles системная переменная.

!!! info "Примечание"
    Вам нужно будет использовать sudo.

Для этого создайте следующий файл:

/Библиотека / LaunchDaemons / limit.параметром maxfiles.файл plist:

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

Выполните следующую команду:

``` bash
$ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Перезагрузить.

Чтобы проверить, работает ли он, вы можете использовать `ulimit -n` команда.

[Оригинальная статья](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->
