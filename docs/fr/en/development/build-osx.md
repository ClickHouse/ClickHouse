---
description: 'Guide pour compiler ClickHouse à partir du code source sur des systèmes macOS'
sidebar_label: 'Compiler sur macOS pour macOS'
sidebar_position: 15
slug: /development/build-osx
title: 'Compiler sur macOS pour macOS'
keywords: ['MacOS', 'Mac', 'compilation']
doc_type: 'guide'
---

:::info Ce guide de compilation s’adresse aux contributeurs qui modifient ClickHouse lui-même.
Si vous ne modifiez pas le code source de ClickHouse, vous pouvez installer une version précompilée de ClickHouse comme décrit dans le [Quick Start](https://clickhouse.com/docs/get-started/quick-start).
:::

ClickHouse peut être compilé sur macOS x86&#95;64 (Intel) et arm64 (Apple Silicon), avec macOS 10.15 (Catalina) ou version ultérieure.

Seul Clang fourni par Homebrew est pris en charge comme compilateur.

<div id="install-prerequisites">
  ## Installer les prérequis
</div>

Commencez par consulter la [documentation générale des prérequis](developer-instruction.md).

Ensuite, installez [Homebrew](https://brew.sh/) et exécutez

Puis exécutez :

```bash
brew update
brew install ccache cmake ninja libtool gettext llvm lld binutils grep findutils nasm bash rust rustup
```

:::note
Apple utilise par défaut un système de fichiers insensible à la casse. Même si cela n’affecte généralement pas la compilation (en particulier, les compilations complètes fonctionneront), cela peut perturber des opérations sur les fichiers comme `git mv`.
Pour un développement sérieux sur macOS, assurez-vous que le code source est stocké sur un volume de disque sensible à la casse ; consultez par exemple [ces instructions](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830).
:::

<div id="build-clickhouse">
  ## Compiler ClickHouse
</div>

Pour compiler, vous devez utiliser le compilateur Clang de Homebrew :

```bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -S . -B build
cmake --build build
# The resulting binary will be created at: build/programs/clickhouse
```

:::note
Si vous rencontrez des erreurs `ld: archive member '/' not a mach-o file in ...` lors de l’édition de liens, vous devrez peut-être
utiliser llvm-ar en définissant l’option `-DCMAKE_AR=/opt/homebrew/opt/llvm/bin/llvm-ar`.
:::

<div id="caveats">
  ## Points à noter
</div>

Si vous comptez exécuter `clickhouse-server`, veillez à augmenter la variable système `maxfiles`.

:::note
Vous devrez utiliser sudo.
:::

Pour ce faire, créez le fichier `/Library/LaunchDaemons/limit.maxfiles.plist` avec le contenu suivant :

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

Définissez les permissions appropriées pour le fichier :

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Vérifiez que le fichier est valide :

```bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

Chargez le fichier (ou redémarrez) :

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

Pour vérifier que tout fonctionne correctement, utilisez les commandes `ulimit -n` ou `launchctl limit maxfiles`.