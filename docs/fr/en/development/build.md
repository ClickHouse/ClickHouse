---
description: 'Guide étape par étape pour compiler ClickHouse à partir des sources sur des systèmes Linux'
sidebar_label: 'Compiler sur Linux'
sidebar_position: 10
slug: /development/build
title: 'Comment compiler ClickHouse sur Linux'
doc_type: 'guide'
---

:::info Ce guide de compilation s&#39;adresse aux contributeurs qui modifient directement ClickHouse.
Si vous ne modifiez pas le code source de ClickHouse, vous pouvez installer une version précompilée de ClickHouse comme indiqué dans le [Quick Start](https://clickhouse.com/docs/get-started/quick-start).
:::

ClickHouse peut être compilé sur les plateformes suivantes :

* x86&#95;64
* AArch64
* PowerPC 64 LE (expérimental)
* s390/x (expérimental)
* RISC-V 64 (expérimental)

<div id="assumptions">
  ## Prérequis
</div>

Le tutoriel suivant est basé sur Ubuntu Linux, mais il devrait également fonctionner sur toute autre distribution Linux moyennant les adaptations appropriées.
La version minimale recommandée d’Ubuntu pour le développement est la 24.04 LTS.

Le tutoriel suppose que vous avez récupéré localement le dépôt ClickHouse et tous ses sous-modules.

<div id="install-prerequisites">
  ## Installer les prérequis
</div>

Commencez par consulter la [documentation générale sur les prérequis](developer-instruction.md).

ClickHouse utilise CMake et Ninja pour la compilation.

Vous pouvez également installer ccache afin de permettre à la compilation de réutiliser des fichiers objets déjà compilés.

```bash
sudo apt-get update
sudo apt-get install build-essential git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg
```

<div id="install-the-clang-compiler">
  ## Installer le compilateur Clang
</div>

Pour installer Clang sur Ubuntu/Debian, utilisez le script d’installation automatique de LLVM disponible [ici](https://apt.llvm.org/).

```bash
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 21
```

Pour les autres distributions Linux, vérifiez si vous pouvez installer l’un des [paquets précompilés](https://releases.llvm.org/download.html) de LLVM.

À compter de février 2026, Clang 21 ou une version ultérieure est requis.
GCC et les autres compilateurs ne sont pas pris en charge.

<div id="install-the-rust-compiler-optional">
  ## Installer le compilateur Rust (facultatif)
</div>

:::note
Rust est une dépendance facultative de ClickHouse.
Si Rust n’est pas installé, certaines fonctionnalités de ClickHouse ne seront pas compilées.
:::

Commencez par suivre les étapes de la [documentation officielle de Rust](https://www.rust-lang.org/tools/install) pour installer `rustup`.

Comme pour les dépendances C++, ClickHouse utilise le vendoring pour maîtriser exactement ce qui est installé et éviter de dépendre de services tiers (comme le registry `crates.io`).

Bien qu’en mode release, toute version récente de la toolchain rustup doive fonctionner avec ces dépendances, si vous prévoyez d’activer des sanitizers, vous devez utiliser une version qui correspond exactement au même `std` que celui utilisé en intégration continue (pour lequel nous incluons les crates via vendoring) :

```bash
rustup toolchain install nightly-2026-03-22
rustup default nightly-2026-03-22
rustup component add rust-src
```

<div id="build-clickhouse">
  ## Compiler ClickHouse
</div>

Nous recommandons de créer un répertoire `build` distinct dans `ClickHouse` pour y stocker tous les artefacts de build :

```sh
mkdir build
cd build
```

Vous pouvez avoir plusieurs répertoires (par ex. `build_release`, `build_debug`, etc.) pour différents types de compilation.

Facultatif : si vous avez installé plusieurs versions du compilateur, vous pouvez préciser exactement quel compilateur utiliser.

```sh
export CC=clang-21
export CXX=clang++-21
```

Pour le développement, les builds de débogage sont recommandés.
Par rapport aux builds de release, ils utilisent un niveau d&#39;optimisation du compilateur (`-O`) inférieur, ce qui offre une meilleure expérience de débogage.
De plus, les exceptions internes de type `LOGICAL_ERROR` provoquent immédiatement un crash au lieu d&#39;échouer proprement.

```sh
cmake -D CMAKE_BUILD_TYPE=Debug ..
```

:::note
Si vous souhaitez utiliser un débogueur tel que gdb, ajoutez `-D DEBUG_O_LEVEL="0"` à la commande ci-dessus afin de supprimer toutes les optimisations du compilateur, qui peuvent empêcher gdb de visualiser les variables ou d’y accéder.
:::

Exécutez ninja pour compiler :

```sh
ninja clickhouse
```

Si vous souhaitez compiler tous les binaires (utilitaires et tests), exécutez ninja sans paramètres :

```sh
ninja
```

Vous pouvez contrôler le nombre de tâches de compilation parallèles à l’aide du paramètre `-j` :

```sh
ninja -j 1 clickhouse
```

:::note
`clickhouse-server`, `clickhouse-client` et les binaires similaires sont des liens symboliques dans le répertoire `programs/` qui pointent vers l’exécutable `clickhouse` une fois le build terminé.

:::tip
CMake fournit des raccourcis pour les commandes ci-dessus :

```sh
cmake -S . -B build  # configure build, run from repository top-level directory
cmake --build build  # compile
```

:::

<div id="running-the-clickhouse-executable">
  ## Exécuter l’exécutable ClickHouse
</div>

Une fois la compilation terminée avec succès, vous trouverez l’exécutable dans `ClickHouse/<build_dir>/programs/` :

Le serveur ClickHouse tente de trouver un fichier de configuration `config.xml` dans le répertoire courant.
Vous pouvez également spécifier un fichier de configuration sur la ligne de commande avec `-C`.

Pour vous connecter au serveur ClickHouse avec `clickhouse-client`, ouvrez un autre terminal, accédez à `ClickHouse/build/programs/` et exécutez `./clickhouse client`.

Si vous obtenez le message `Connection refused` sur macOS ou FreeBSD, essayez de spécifier l’adresse 127.0.0.1 :

```bash
clickhouse client --host 127.0.0.1
```

<div id="advanced-options">
  ## Options avancées
</div>

<div id="minimal-build">
  ### Compilation minimale
</div>

Si vous n’avez pas besoin des fonctionnalités fournies par des bibliothèques tierces, vous pouvez encore accélérer la compilation :

```sh
cmake -DENABLE_LIBRARIES=OFF
```

En cas de problème, vous êtes livré à vous-même…

Rust nécessite une connexion Internet. Pour désactiver la prise en charge de Rust :

```sh
cmake -DENABLE_RUST=OFF
```

<div id="running-the-clickhouse-executable-1">
  ### Utilisation de l’exécutable ClickHouse
</div>

Vous pouvez remplacer la version de production du binaire ClickHouse installée sur votre système par le binaire ClickHouse compilé.
Pour ce faire, installez ClickHouse sur votre machine en suivant les instructions du site officiel.
Ensuite, exécutez :

```bash
sudo service clickhouse-server stop
sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
sudo service clickhouse-server start
```

Notez que `clickhouse-client`, `clickhouse-server` et d&#39;autres sont des liens symboliques vers le binaire `clickhouse` commun.

Vous pouvez également exécuter votre binaire ClickHouse compilé sur mesure avec le fichier de configuration du paquet ClickHouse installé sur votre système :

```bash
sudo service clickhouse-server stop
sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

<div id="building-on-any-linux">
  ### Compilation sous n’importe quel Linux
</div>

Installez les prérequis sur OpenSUSE Tumbleweed :

```bash
sudo zypper install git cmake ninja clang-c++ python lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

Installez les prérequis sur Fedora Rawhide :

```bash
sudo yum update
sudo yum --nogpg install git cmake make clang python3 ccache lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

<div id="building-in-docker">
  ### Compilation avec Docker
</div>

Vous pouvez exécuter n’importe quelle compilation localement dans un environnement similaire à celui de l’intégration continue à l’aide de :

```bash
python -m ci.praktika run "BUILD_JOB_NAME"
```

où BUILD&#95;JOB&#95;NAME est le nom du job tel qu’il apparaît dans le rapport d’intégration continue, par exemple &quot;Build (arm&#95;release)&quot;, &quot;Build (amd&#95;debug)&quot;

Cette commande télécharge l’image Docker appropriée `clickhouse/binary-builder` avec toutes les dépendances requises,
et exécute le script de build à l’intérieur : `./ci/jobs/build_clickhouse.py`

La sortie du build sera placée dans `./ci/tmp/`.

Elle fonctionne sur les architectures AMD et ARM et ne nécessite aucune dépendance supplémentaire en dehors de Python avec le module `requests` disponible et de Docker.