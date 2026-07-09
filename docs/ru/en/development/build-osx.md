---
description: 'Руководство по сборке ClickHouse из исходного кода в macOS'
sidebar_label: 'Сборка на macOS для macOS'
sidebar_position: 15
slug: /development/build-osx
title: 'Сборка на macOS для macOS'
keywords: ['MacOS', 'Mac', 'build']
doc_type: 'guide'
---

:::info Это руководство по сборке предназначено для контрибьюторов, которые вносят изменения в сам ClickHouse.
Если вы не изменяете исходный код ClickHouse, можно установить готовую сборку ClickHouse, как описано в [Быстрый старт](https://clickhouse.com/docs/get-started/quick-start).
:::

ClickHouse можно компилировать на macOS x86&#95;64 (Intel) и arm64 (Apple Silicon) в macOS 10.15 (Catalina) и выше.

В качестве компилятора поддерживается только Clang из Homebrew.

<div id="install-prerequisites">
  ## Установите необходимые компоненты
</div>

Сначала ознакомьтесь с общей [документацией по необходимым компонентам](developer-instruction.md).

Затем установите [Homebrew](https://brew.sh/) и выполните

После этого выполните:

```bash
brew update
brew install ccache cmake ninja libtool gettext llvm lld binutils grep findutils nasm bash rust rustup
```

:::note
Apple по умолчанию использует регистронезависимую файловую систему. Хотя обычно это не влияет на компиляцию (особенно если собирать с нуля), при файловых операциях, таких как `git mv`, это может вызывать проблемы.
Для серьёзной разработки на macOS убедитесь, что исходный код хранится на регистрозависимом дисковом томе; например, см. [эти инструкции](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830).
:::

<div id="build-clickhouse">
  ## Сборка ClickHouse
</div>

Для сборки необходимо использовать компилятор Clang из Homebrew:

```bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -S . -B build
cmake --build build
# The resulting binary will be created at: build/programs/clickhouse
```

:::note
Если при компоновке у вас возникают ошибки `ld: archive member '/' not a mach-o file in ...`, возможно, потребуется
использовать llvm-ar, указав флаг `-DCMAKE_AR=/opt/homebrew/opt/llvm/bin/llvm-ar`.
:::

<div id="caveats">
  ## Важные замечания
</div>

Если вы планируете запускать `clickhouse-server`, обязательно увеличьте значение системной переменной `maxfiles`.

:::note
Для этого потребуются права sudo.
:::

Для этого создайте файл `/Library/LaunchDaemons/limit.maxfiles.plist` со следующим содержимым:

```xml
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

Назначьте файлу правильные права доступа:

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Проверьте, что файл корректен:

```bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

Загрузите файл (или перезагрузите систему):

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

Чтобы проверить, всё ли работает, используйте команды `ulimit -n` или `launchctl limit maxfiles`.