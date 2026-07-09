---
description: 's390x アーキテクチャ向けに ClickHouse をソースからビルドするガイド'
sidebar_label: 'Linux で s390x（zLinux）向けにビルド'
sidebar_position: 30
slug: /development/build-cross-s390x
title: 'Linux で s390x（zLinux）向けにビルド'
doc_type: 'guide'
---

ClickHouse は s390x を実験的にサポートしています。

<div id="building-clickhouse-for-s390x">
  ## s390x 向けに ClickHouse をビルドする
</div>

s390x では、他のプラットフォームと同様に、OpenSSL は静的ライブラリとしてビルドされます。動的 OpenSSL でビルドする場合は、CMake に `-DENABLE_OPENSSL_DYNAMIC=1` を渡す必要があります。

これらの手順は、ホストマシンが Linux x86&#95;64/ARM であり、[ビルド手順](../development/build.md)に従ってネイティブにビルドするために必要なツール一式がそろっていることを前提としています。また、ホストは Ubuntu 22.04 であることを前提としていますが、以下の手順は Ubuntu 20.04 でも動作するはずです。

ネイティブビルドに使用するツールのインストールに加えて、以下の追加パッケージもインストールする必要があります。

```bash
apt-get mold
rustup target add s390x-unknown-linux-gnu
```

s390x向けにビルドするには:

```bash
cmake -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-s390x.cmake ..
ninja
```

<div id="running">
  ## 実行
</div>

エミュレーションを行うには、s390x 向けの QEMU user static binary が必要です。Ubuntu では次のコマンドでインストールできます。

```bash
apt-get install binfmt-support binutils-s390x-linux-gnu qemu-user-static
```

ビルドが完了したら、たとえば次のようにバイナリを実行できます。

```bash
qemu-s390x-static -L /usr/s390x-linux-gnu ./programs/clickhouse local --query "Select 2"
2
```

<div id="debugging">
  ## デバッグ
</div>

LLDB をインストールします。

```bash
apt-get install lldb-21
```

s390x の実行可能ファイルをデバッグするには、QEMU のデバッグモードで clickhouse を実行します。

```bash
qemu-s390x-static -g 31338 -L /usr/s390x-linux-gnu ./clickhouse
```

別のシェルでLLDBを実行してアタッチし、`<Clickhouse Parent Directory>` と `<build directory>` はご利用の環境に応じた値に置き換えてください。

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
  ## Visual Studio Code インテグレーション
</div>

* ビジュアルデバッグには、[CodeLLDB](https://github.com/vadimcn/vscode-lldb) 拡張機能が必要です。
* [CMake Variants](https://github.com/microsoft/vscode-cmake-tools/blob/main/docs/variants.md) を使用している場合は、[Command Variable](https://github.com/rioj7/command-variable) 拡張機能が動的な起動に役立ちます。
* backend が LLVM のインストール先を参照するように設定してください。例: `"lldb.library": "/usr/lib/x86_64-linux-gnu/liblldb-21.so"`
* 起動前に、必ず `clickhouse` 実行可能ファイルをデバッグモードで実行してください。 (これを自動化する `preLaunchTask` を作成することもできます)

<div id="example-configurations">
  ### 設定例
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

これにより、異なるビルドは `build` フォルダ内の別々のサブフォルダに配置されます。

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

コンパイル済みの実行可能ファイルを `server` モードで実行するタスクを定義します。設定には `programs/server/config.xml` を使用し、実行場所はバイナリの隣にある `tmp` フォルダー内です。

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