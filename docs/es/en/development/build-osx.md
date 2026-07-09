---
description: 'Guía para compilar ClickHouse desde el código fuente en sistemas macOS'
sidebar_label: 'Compilar en macOS para macOS'
sidebar_position: 15
slug: /development/build-osx
title: 'Compilar en macOS para macOS'
keywords: ['MacOS', 'Mac', 'build']
doc_type: 'guide'
---

:::info Esta guía de compilación está dirigida a colaboradores que modifican el propio ClickHouse.
Si no va a cambiar el código fuente de ClickHouse, puede instalar una versión precompilada de ClickHouse, como se describe en [Quick Start](https://clickhouse.com/docs/get-started/quick-start).
:::

ClickHouse se puede compilar en macOS x86&#95;64 (Intel) y arm64 (Apple Silicon) con macOS 10.15 (Catalina) o superior.

Solo se admite Clang de Homebrew como compilador.

<div id="install-prerequisites">
  ## Instala los requisitos previos
</div>

Primero, consulta la [documentación general sobre requisitos previos](developer-instruction.md).

A continuación, instala [Homebrew](https://brew.sh/) y ejecuta

Después, ejecuta:

```bash
brew update
brew install ccache cmake ninja libtool gettext llvm lld binutils grep findutils nasm bash rust rustup
```

:::note
Apple usa de forma predeterminada un sistema de archivos que no distingue entre mayúsculas y minúsculas. Aunque esto normalmente no afecta a la compilación (especialmente las compilaciones desde cero funcionarán), puede causar problemas en operaciones con archivos como `git mv`.
Para un desarrollo serio en macOS, asegúrate de que el código fuente esté almacenado en un volumen de disco sensible a mayúsculas y minúsculas; por ejemplo, consulta [estas instrucciones](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830).
:::

<div id="build-clickhouse">
  ## Compilar ClickHouse
</div>

Para compilarlo, debe usar el compilador Clang de Homebrew:

```bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -S . -B build
cmake --build build
# The resulting binary will be created at: build/programs/clickhouse
```

:::note
Si aparece el error `ld: archive member '/' not a mach-o file in ...` durante el enlazado, puede que necesites
usar llvm-ar estableciendo el indicador `-DCMAKE_AR=/opt/homebrew/opt/llvm/bin/llvm-ar`.
:::

<div id="caveats">
  ## Consideraciones
</div>

Si tiene previsto ejecutar `clickhouse-server`, asegúrese de aumentar la variable `maxfiles` del sistema.

:::note
Necesitará usar `sudo`.
:::

Para ello, cree el archivo `/Library/LaunchDaemons/limit.maxfiles.plist` con el siguiente contenido:

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

Asigna al archivo los permisos correctos:

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Verifique que el archivo sea correcto:

```bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

Cargue el archivo (o reinicie):

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

Para comprobar si funciona, usa los comandos `ulimit -n` o `launchctl limit maxfiles`.