---
description: 'Руководство по сборке ClickHouse из исходного кода для архитектуры s390x'
sidebar_label: 'Сборка в Linux для s390x (zLinux)'
sidebar_position: 30
slug: /development/build-cross-s390x
title: 'Сборка в Linux для s390x (zLinux)'
doc_type: 'guide'
---

ClickHouse поддерживает s390x в экспериментальном режиме.

<div id="building-clickhouse-for-s390x">
  ## Сборка ClickHouse для s390x
</div>

На платформе s390x, как и на других платформах, OpenSSL собирается как статическая библиотека. Если вы хотите собрать с динамическим OpenSSL, передайте `-DENABLE_OPENSSL_DYNAMIC=1` в CMake.

В этих инструкциях предполагается, что хостовая машина работает под Linux x86&#95;64/ARM и на ней установлены все инструменты, необходимые для нативной сборки согласно [инструкциям по сборке](../development/build.md). Также предполагается, что в качестве хоста используется Ubuntu 22.04, однако приведённые ниже инструкции должны работать и на Ubuntu 20.04.

Помимо установки инструментов, используемых для нативной сборки, необходимо установить следующие дополнительные пакеты:

```bash
apt-get mold
rustup target add s390x-unknown-linux-gnu
```

Чтобы собрать для s390x:

```bash
cmake -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-s390x.cmake ..
ninja
```

<div id="running">
  ## Запуск
</div>

Для эмуляции вам потребуется статический бинарный файл QEMU user для s390x. В Ubuntu его можно установить так:

```bash
apt-get install binfmt-support binutils-s390x-linux-gnu qemu-user-static
```

После сборки бинарный файл можно запустить, например, так:

```bash
qemu-s390x-static -L /usr/s390x-linux-gnu ./programs/clickhouse local --query "Select 2"
2
```

<div id="debugging">
  ## Отладка
</div>

Установите LLDB:

```bash
apt-get install lldb-21
```

Чтобы отладить исполняемый файл s390x, запустите clickhouse через QEMU в режиме отладки:

```bash
qemu-s390x-static -g 31338 -L /usr/s390x-linux-gnu ./clickhouse
```

В другом терминале запустите LLDB и подключитесь к процессу, заменив `<Clickhouse Parent Directory>` и `<build directory>` на значения, соответствующие вашей среде.

```bash
lldb-15
(lldb) target create ./clickhouse
Current executable set to '/<Clickhouse Parent Directory>/ClickHouse/<build directory>/programs/clickhouse' (s390x).
(lldb) settings set target.source-map <build directory> /<Clickhouse Parent Directory>/ClickHouse
(lldb) gdb-remote 31338
Process 1 stopped
* thread #1, stop reason = signal SIGTRAP
    frame #0: 0x0000004020e74cd0
->  0x4020e74cd0: lgr    %r2, %r15
    0x4020e74cd4: aghi   %r15, -160
    0x4020e74cd8: xc     0(8,%r15), 0(%r15)
    0x4020e74cde: brasl  %r14, 275429939040
(lldb) b main
Breakpoint 1: 9 locations.
(lldb) c
Process 1 resuming
Process 1 stopped
* thread #1, stop reason = breakpoint 1.1
    frame #0: 0x0000004005cd9fc0 clickhouse`main(argc_=1, argv_=0x0000004020e594a8) at main.cpp:450:17
   447  #if !defined(FUZZING_MODE)
   448  int main(int argc_, char ** argv_)
   449  {
-> 450      inside_main = true;
   451      SCOPE_EXIT({ inside_main = false; });
   452
   453      /// PHDR cache is required for query profiler to work reliably
```

<div id="visual-studio-code-integration">
  ## Интеграция с Visual Studio Code
</div>

* Для визуальной отладки требуется расширение [CodeLLDB](https://github.com/vadimcn/vscode-lldb).
* Расширение [Command Variable](https://github.com/rioj7/command-variable) может быть полезно для динамического запуска при использовании [CMake Variants](https://github.com/microsoft/vscode-cmake-tools/blob/main/docs/variants.md).
* Обязательно укажите в качестве бэкенда вашу установку LLVM, например: `"lldb.library": "/usr/lib/x86_64-linux-gnu/liblldb-21.so"`
* Перед запуском обязательно запустите исполняемый файл clickhouse в режиме отладки. (Также можно создать `preLaunchTask`, чтобы автоматизировать это)

<div id="example-configurations">
  ### Примеры конфигураций
</div>

<div id="cmake-variantsyaml">
  #### cmake-variants.yaml
</div>

```yaml
buildType:
  default: relwithdebinfo
  choices:
    debug:
      short: Debug
      long: Emit debug information
      buildType: Debug
    release:
      short: Release
      long: Optimize generated code
      buildType: Release
    relwithdebinfo:
      short: RelWithDebInfo
      long: Release with Debug Info
      buildType: RelWithDebInfo
    tsan:
      short: MinSizeRel
      long: Minimum Size Release
      buildType: MinSizeRel

toolchain:
  default: default
  description: Select toolchain
  choices:
    default:
      short: x86_64
      long: x86_64
    s390x:
      short: s390x
      long: s390x
      settings:
        CMAKE_TOOLCHAIN_FILE: cmake/linux/toolchain-s390x.cmake
```

<div id="launchjson">
  #### launch.json
</div>

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "type": "lldb",
            "request": "custom",
            "name": "(lldb) Launch s390x with qemu",
            "targetCreateCommands": ["target create ${command:cmake.launchTargetPath}"],
            "processCreateCommands": ["gdb-remote 2159"],
            "preLaunchTask": "Run ClickHouse"
        }
    ]
}
```

<div id="settingsjson">
  #### settings.json
</div>

Это также поместит разные сборки в разные подпапки каталога `build`.

```json
{
    "cmake.buildDirectory": "${workspaceFolder}/build/${buildKitVendor}-${buildKitVersion}-${variant:toolchain}-${variant:buildType}",
    "lldb.library": "/usr/lib/x86_64-linux-gnu/liblldb-21.so"
}
```

<div id="run-debugsh">
  #### run-debug.sh
</div>

```sh
#! /bin/sh
echo 'Starting debugger session'
cd $1
qemu-s390x-static -g 2159 -L /usr/s390x-linux-gnu $2 $3 $4
```

<div id="tasksjson">
  #### tasks.json
</div>

Определяет задачу для запуска скомпилированного исполняемого файла в режиме `server` в папке `tmp` рядом с бинарными файлами, с конфигурацией из файла `programs/server/config.xml`.

```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "Run ClickHouse",
            "type": "shell",
            "isBackground": true,
            "command": "${workspaceFolder}/.vscode/run-debug.sh",
            "args": [
                "${command:cmake.launchTargetDirectory}/tmp",
                "${command:cmake.launchTargetPath}",
                "server",
                "--config-file=${workspaceFolder}/programs/server/config.xml"
            ],
            "problemMatcher": [
                {
                    "pattern": [
                        {
                            "regexp": ".",
                            "file": 1,
                            "location": 2,
                            "message": 3
                        }
                    ],
                    "background": {
                        "activeOnStart": true,
                        "beginsPattern": "^Starting debugger session",
                        "endsPattern": ".*"
                    }
                }
            ]
        }
    ]
}
```