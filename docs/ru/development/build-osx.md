---
sidebar_position: 65
sidebar_label: Сборка на Mac OS X
---

# Как собрать ClickHouse на Mac OS X {#how-to-build-clickhouse-on-mac-os-x}

:::info "Вам не нужно собирать ClickHouse самостоятельно"
     Вы можете установить предварительно собранный ClickHouse, как описано в [Быстром старте](https://clickhouse.com/#quick-start).
     Следуйте инструкциям по установке для `macOS (Intel)` или `macOS (Apple Silicon)`.

Сборка должна запускаться с x86_64 (Intel) на macOS версии 10.15 (Catalina) и выше в последней версии компилятора Xcode's native AppleClang, Homebrew's vanilla Clang или в GCC-компиляторах.

## Установка Homebrew {#install-homebrew}

``` bash
$ /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

## Установка Xcode и инструментов командной строки {#install-xcode-and-command-line-tools}

  1. Установите из App Store последнюю версию [Xcode](https://apps.apple.com/am/app/xcode/id497799835?mt=12).

  2. Запустите ее, чтобы принять лицензионное соглашение. Необходимые компоненты установятся автоматически.

  3. Затем убедитесь, что в системе выбрана последняя версия инструментов командной строки:

    ``` bash
    $ sudo rm -rf /Library/Developer/CommandLineTools
    $ sudo xcode-select --install
    ```

  4. Перезагрузитесь.

## Установка компиляторов, инструментов и библиотек {#install-required-compilers-tools-and-libraries}

  ``` bash
  $ brew update
  $ brew install cmake ninja libtool gettext llvm gcc
  ```

## Просмотр исходников ClickHouse {#checkout-clickhouse-sources}

  ``` bash
  $ git clone --recursive git@github.com:ClickHouse/ClickHouse.git # or https://github.com/ClickHouse/ClickHouse.git
  ```

## Сборка ClickHouse {#build-clickhouse}

  Чтобы запустить сборку в компиляторе Xcode's native AppleClang:

  ``` bash
  $ cd ClickHouse
  $ rm -rf build
  $ mkdir build
  $ cd build
  $ cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_JEMALLOC=OFF ..
  $ cmake --build . --config RelWithDebInfo
  $ cd ..
  ```

Чтобы запустить сборку в компиляторе Homebrew's vanilla Clang:

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
  $ cmake -DCMAKE_C_COMPILER=$(brew --prefix gcc)/bin/gcc-11 -DCMAKE_CXX_COMPILER=$(brew --prefix gcc)/bin/g++-11 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_JEMALLOC=OFF ..
  $ cmake --build . --config RelWithDebInfo
  $ cd ..
  ```

## Предупреждения {#caveats}

Если будете запускать `clickhouse-server`, убедитесь, что увеличили системную переменную `maxfiles`.

:::info "Note"
    Вам понадобится команда `sudo`.

1. Создайте файл `/Library/LaunchDaemons/limit.maxfiles.plist` и поместите в него следующее:

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

2. Выполните команду:

  ``` bash
  $ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
  ```

3. Перезагрузитесь.

4. Чтобы проверить, как это работает, выполните команду `ulimit -n`.

[Original article](https://clickhouse.com/docs/en/development/build_osx/) <!--hide-->
