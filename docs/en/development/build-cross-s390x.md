---
slug: /en/development/build-cross-s390x
sidebar_position: 69
title: How to Build, Run and Debug ClickHouse on Linux for s390x (zLinux)
sidebar_label: Build on Linux for s390x (zLinux)
---

At the time of writing (2024 May), support for the s390x platform is considered experimental, i.e. some features are disabled or broken on s390x.

## Building ClickHouse for s390x

s390x has two OpenSSL-related build options:
- By default, OpenSSL is build on s390x as a shared library. This is different from all other platforms, where OpenSSL is build as static library.
- To build OpenSSL as a static library regardless, pass `-DENABLE_OPENSSL_DYNAMIC=0` to CMake.

These instructions assume that the host machine is x86_64 and has all the tooling required to build natively based on the [build instructions](../development/build.md). It also assumes that the host is Ubuntu 22.04 but the following instructions should also work on Ubuntu 20.04.

In addition to installing the tooling used to build natively, the following additional packages need to be installed:

```bash
apt-get install binutils-s390x-linux-gnu libc6-dev-s390x-cross gcc-s390x-linux-gnu binfmt-support qemu-user-static
```

If you wish to cross compile rust code install the rust cross compile target for s390x:

```bash
rustup target add s390x-unknown-linux-gnu
```

The s390x build uses the mold linker, download it from https://github.com/rui314/mold/releases/download/v2.0.0/mold-2.0.0-x86_64-linux.tar.gz
and place it into your `$PATH`.

To build for s390x:

```bash
cmake -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-s390x.cmake ..
ninja
```

## Running

Once built, the binary can be run with, e.g.:

```bash
qemu-s390x-static -L /usr/s390x-linux-gnu ./clickhouse
```

## Debugging

Install LLDB:

```bash
apt-get install lldb-15
```

To Debug a s390x executable, run clickhouse using QEMU in debug mode:

```bash
qemu-s390x-static -g 31338 -L /usr/s390x-linux-gnu ./clickhouse
```

In another shell run LLDB and attach, replace `<Clickhouse Parent Directory>` and `<build directory>` with the values corresponding to your environment.
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

## Visual Studio Code integration

- [CodeLLDB](https://github.com/vadimcn/vscode-lldb) extension is required for visual debugging.
- [Command Variable](https://github.com/rioj7/command-variable) extension can help dynamic launches if using [CMake Variants](https://github.com/microsoft/vscode-cmake-tools/blob/main/docs/variants.md).
- Make sure to set the backend to your LLVM installation eg. `"lldb.library": "/usr/lib/x86_64-linux-gnu/liblldb-15.so"`
- Make sure to run the clickhouse executable in debug mode prior to launch. (It is also possible to create a `preLaunchTask` that automates this)

### Example configurations
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
This would also put different builds under different subfolders of the `build` folder.
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
Defines a task to run the compiled executable in `server` mode under a `tmp` folder next to the binaries, with configuration from under `programs/server/config.xml`.
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
