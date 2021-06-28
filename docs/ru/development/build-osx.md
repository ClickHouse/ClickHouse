---
toc_priority: 65
toc_title: Build on Mac OS X
---
# Как собрать ClickHouse на Mac OS X {#how-to-build-clickhouse-on-mac-os-x}
Сборка должна работать с x86_64 (Intel) на macOS 10.15 (Catalina) и выше с последним компилятором Xcode's native AppleClang, или Homebrew's vanilla Clang или GCC-компиляторами.

## Установка Homebrew {#install-homebrew}

``` bash
$ /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
## Установка Xcode и Инструментов Командной Строки {#install-xcode-and-command-line-tools}

Установите последнюю версию [Xcode](https://apps.apple.com/am/app/xcode/id497799835?mt=12) с App Store.

Откройте ее хотя бы один раз, чтобы принять лицензионное соглашение конечного пользователя и автоматически установить необходимые компоненты.

Дальше убедитесь, что в системе выбрана последняя версия Инструментов Командной Строки:

``` bash
$ sudo rm -rf /Library/Developer/CommandLineTools
$ sudo xcode-select --install
```

Перезагрузитесь.

## Установите необходимые компиляторы, инструменты и библиотеки {#install-required-compilers-tools-and-libraries}

``` bash
$ brew update
$ brew install cmake ninja libtool gettext llvm gcc
```

## Посмотреть исходники ClickHouse {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git # or https://github.com/ClickHouse/ClickHouse.git
```

## Сборка ClickHouse {#build-clickhouse}

Для сборка в компиляторе Xcode's native AppleClang:

``` bash
$ cd ClickHouse
$ rm -rf build
$ mkdir build
$ cd build
$ cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_JEMALLOC=OFF ..
$ cmake --build . --config RelWithDebInfo
$ cd ..
```

Сборка с помощью компилятора Homebrew's vanilla Clang:

``` bash
$ cd ClickHouse
$ rm -rf build
$ mkdir build
$ cd build
$ cmake -DCMAKE_C_COMPILER=$(brew --prefix llvm)/bin/clang -DCMAKE_CXX_COMPILER==$(brew --prefix llvm)/bin/clang++ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_JEMALLOC=OFF ..
$ cmake -DCMAKE_C_COMPILER=$(brew --prefix llvm)/bin/clang -DCMAKE_CXX_COMPILER=$(brew --prefix llvm)/bin/clang++ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_JEMALLOC=OFF ..
$ cmake --build . --config RelWithDebInfo
$ cd ..
```

Чтобы собрать с помощью компилятора Homebrew's vanilla GCC:

``` bash
$ cd ClickHouse
$ rm -rf build
$ mkdir build
$ cd build
$ cmake -DCMAKE_C_COMPILER=$(brew --prefix gcc)/bin/gcc-10 -DCMAKE_CXX_COMPILER=$(brew --prefix gcc)/bin/g++-10 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_JEMALLOC=OFF ..
$ cmake --build . --config RelWithDebInfo
$ cd ..
```
## Caveats {#caveats}

Если вы планируете запускать `clickhouse-server`, убедитесь, что увеличили системную переменную maxfiles.

!!! info "Note"
    Вам понадобится sudo.

Чтобы так сделать, создайте файл `/Library/LaunchDaemons/limit.maxfiles.plist` и поместите в него следующее:

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

Перезагрузитесь.

Чтобы проверить, как это работает, пользуйтесь командой `ulimit -n`.
[Original article](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->