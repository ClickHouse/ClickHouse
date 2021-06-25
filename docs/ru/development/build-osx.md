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

Откройте ее хотя бы один разЮ чтобы принять лицензионное соглашение конечного пользователя и автоматически установить необходимые компоненты.

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

## Checkout ClickHouse Sources {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git # or https://github.com/ClickHouse/ClickHouse.git
```

## Сборка ClickHouse {#build-clickhouse}

To build using Xcode's native AppleClang compiler:

``` bash
$ cd ClickHouse
$ rm -rf build
$ mkdir build
$ cd build
$ cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_JEMALLOC=OFF ..
$ cmake --build . --config RelWithDebInfo
$ cd ..
```

To build using Homebrew's vanilla Clang compiler:

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
To build using Homebrew's vanilla GCC compiler:
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

If you intend to run `clickhouse-server`, make sure to increase the system’s maxfiles variable.

!!! info "Note"
    You’ll need to use sudo.

To do so, create the `/Library/LaunchDaemons/limit.maxfiles.plist` file with the following content:

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

Execute the following command:

``` bash
$ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Reboot.

To check if it’s working, you can use `ulimit -n` command.
[Original article](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->