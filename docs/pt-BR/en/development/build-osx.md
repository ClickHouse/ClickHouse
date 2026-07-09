---
description: 'Guia para compilar o ClickHouse a partir do código-fonte em sistemas macOS'
sidebar_label: 'Compilar no macOS para macOS'
sidebar_position: 15
slug: /development/build-osx
title: 'Compilar no macOS para macOS'
keywords: ['MacOS', 'Mac', 'build']
doc_type: 'guide'
---

:::info Este guia de compilação é destinado a colaboradores que modificam o próprio ClickHouse.
Se você não estiver alterando o código-fonte do ClickHouse, poderá instalar a versão pré-compilada do ClickHouse conforme descrito no [Quick Start](https://clickhouse.com/docs/get-started/quick-start).
:::

O ClickHouse pode ser compilado em macOS x86&#95;64 (Intel) e arm64 (Apple Silicon), no macOS 10.15 (Catalina) ou superior.

Como compilador, apenas o Clang do Homebrew é compatível.

<div id="install-prerequisites">
  ## Instale os pré-requisitos
</div>

Primeiro, consulte a [documentação geral de pré-requisitos](developer-instruction.md).

Em seguida, instale o [Homebrew](https://brew.sh/) e execute

Depois, execute:

```bash
brew update
brew install ccache cmake ninja libtool gettext llvm lld binutils grep findutils nasm bash rust rustup
```

:::note
A Apple usa, por padrão, um sistema de arquivos que não diferencia maiúsculas de minúsculas. Embora isso normalmente não afete a compilação (especialmente em compilações do zero), pode causar confusão em operações de arquivo, como `git mv`.
Para desenvolvimento sério no macOS, certifique-se de que o código-fonte esteja armazenado em um volume de disco que diferencie maiúsculas de minúsculas; por exemplo, consulte [estas instruções](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830).
:::

<div id="build-clickhouse">
  ## Compile o ClickHouse
</div>

Para compilar, é necessário usar o compilador Clang do Homebrew:

```bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -S . -B build
cmake --build build
# The resulting binary will be created at: build/programs/clickhouse
```

:::note
Se você estiver encontrando erros `ld: archive member '/' not a mach-o file in ...` durante a etapa de linkedição, talvez seja necessário
usar o llvm-ar definindo a flag `-DCMAKE_AR=/opt/homebrew/opt/llvm/bin/llvm-ar`.
:::

<div id="caveats">
  ## Observações
</div>

Se você pretende executar o `clickhouse-server`, certifique-se de aumentar a variável `maxfiles` do sistema.

:::note
Você precisará usar `sudo`.
:::

Para isso, crie o arquivo `/Library/LaunchDaemons/limit.maxfiles.plist` com o seguinte conteúdo:

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

Defina as permissões corretas para o arquivo:

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Verifique se o arquivo está correto:

```bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

Carregue o arquivo (ou reinicie):

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

Para verificar se está funcionando, use os comandos `ulimit -n` ou `launchctl limit maxfiles`.