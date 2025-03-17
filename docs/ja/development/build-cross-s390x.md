---
slug: /ja/development/build-cross-s390x
sidebar_position: 69
title: LinuxでのClickHouseのビルド、実行、およびデバッグ（s390x用、zLinux）
sidebar_label: Linuxでのs390x（zLinux）用ビルド
---

執筆時点（2024年5月）では、s390xプラットフォームのサポートはエクスペリメンタルなものとされており、一部機能はs390xでは無効化または不具合が発生しています。

## s390x向けClickHouseのビルド

s390xには2つのOpenSSL関連のビルドオプションがあります：
- デフォルトでは、OpenSSLはs390x上で共有ライブラリとしてビルドされます。これは他のすべてのプラットフォームと異なり、OpenSSLは静的ライブラリとしてビルドされます。
- OpenSSLを静的ライブラリとしてビルドするには、CMakeに`-DENABLE_OPENSSL_DYNAMIC=0`を渡します。

これらの指示は、ホストマシンがx86_64であり、[ビルド手順](../development/build.md)に基づいてネイティブにビルドするために必要なツールをすべて備えていると想定しています。また、ホストがUbuntu 22.04であることを前提としていますが、以下の指示はUbuntu 20.04でも動作するはずです。

ネイティブにビルドするために使用されるツールをインストールすることに加えて、以下の追加パッケージが必要です：

```bash
apt-get install binutils-s390x-linux-gnu libc6-dev-s390x-cross gcc-s390x-linux-gnu binfmt-support qemu-user-static
```

Rustコードをクロスコンパイルしたい場合は、s390x用のRustクロスコンパイルターゲットをインストールします：

```bash
rustup target add s390x-unknown-linux-gnu
```

s390xのビルドにはmoldリンカを使用します。https://github.com/rui314/mold/releases/download/v2.0.0/mold-2.0.0-x86_64-linux.tar.gz からダウンロードし、`$PATH`に配置してください。

s390x向けにビルドするには：

```bash
cmake -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-s390x.cmake ..
ninja
```

## 実行

ビルドが完了したら、次のコマンドでバイナリを実行できます：

```bash
qemu-s390x-static -L /usr/s390x-linux-gnu ./clickhouse
```

## デバッグ

LLDBをインストールします：

```bash
apt-get install lldb-15
```

s390xの実行ファイルをデバッグするには、QEMUのデバッグモードでclickhouseを実行します：

```bash
qemu-s390x-static -g 31338 -L /usr/s390x-linux-gnu ./clickhouse
```

別のシェルでLLDBを実行してアタッチします。`<Clickhouse Parent Directory>`と`<build directory>`を環境に合った値に置き換えてください。

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

## Visual Studio Code統合

- ビジュアルデバッグには[CodeLLDB](https://github.com/vadimcn/vscode-lldb)拡張機能が必要です。
- [Command Variable](https://github.com/rioj7/command-variable)拡張機能を使用すると、[CMake Variants](https://github.com/microsoft/vscode-cmake-tools/blob/main/docs/variants.md)を使用して動的な起動を助けることができます。
- LLVMをインストールしているバックエンドを必ず設定してください（例：`"lldb.library": "/usr/lib/x86_64-linux-gnu/liblldb-15.so"`）。
- 起動前に必ずデバッグモードでclickhouse実行ファイルを実行してください。（これを自動化する`preLaunchTask`を作成することも可能です。）

### 例の設定
#### cmake-variants.yaml
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

#### launch.json
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

#### settings.json
異なるビルドを`build`フォルダ内の異なるサブフォルダに配置する設定です。
```json
{
    "cmake.buildDirectory": "${workspaceFolder}/build/${buildKitVendor}-${buildKitVersion}-${variant:toolchain}-${variant:buildType}",
    "lldb.library": "/usr/lib/x86_64-linux-gnu/liblldb-15.so"
}
```

#### run-debug.sh
```sh
#! /bin/sh
echo 'Starting debugger session'
cd $1
qemu-s390x-static -g 2159 -L /usr/s390x-linux-gnu $2 $3 $4
```

#### tasks.json
`tmp`フォルダ内で`server`モードでコンパイルされた実行ファイルを、`programs/server/config.xml`の設定で実行するタスクを定義します。
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
