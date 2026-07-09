---
description: 'Page détaillant le profilage des allocations dans ClickHouse'
sidebar_label: 'Profilage des allocations pour les versions antérieures à 25.9'
slug: /operations/allocation-profiling-old
title: 'Profilage des allocations pour les versions antérieures à 25.9'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling-for-versions-before-259">
  # Profilage des allocations pour les versions antérieures à 25.9
</div>

ClickHouse utilise [jemalloc](https://github.com/jemalloc/jemalloc) comme allocateur global. Jemalloc fournit également des outils d’échantillonnage et de profilage des allocations.
Pour faciliter le profilage des allocations, des commandes `SYSTEM` sont proposées, ainsi que des commandes à quatre lettres (4LW) dans Keeper.

<div id="sampling-allocations-and-flushing-heap-profiles">
  ## Échantillonnage des allocations et écriture des profils mémoire
</div>

Si vous souhaitez échantillonner et profiler les allocations dans `jemalloc`, vous devez démarrer ClickHouse/Keeper en activant le profilage via la variable d’environnement `MALLOC_CONF` :

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:true
```

`jemalloc` échantillonne les allocations et stocke les informations en interne.

Vous pouvez demander à `jemalloc` de vider le profil actuel en exécutant :

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Par défaut, le fichier de profil mémoire est généré dans `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`, où `_pid_` correspond au PID de ClickHouse et `_seqnum_` au numéro de séquence global du profil mémoire actuel.
Pour Keeper, le fichier par défaut est `/tmp/jemalloc_keeper._pid_._seqnum_.heap` et suit les mêmes règles.

Vous pouvez définir un autre emplacement en ajoutant l’option `prof_prefix` à la variable d’environnement `MALLOC_CONF`.
Par exemple, si vous souhaitez générer des profiles dans le dossier `/data` avec `my_current_profile` comme préfixe de nom de fichier, vous pouvez exécuter ClickHouse/Keeper avec la variable d’environnement suivante :

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```

Le fichier généré se verra ajouter le préfixe PID et un numéro de séquence.

<div id="analyzing-heap-profiles">
  ## Analyse des profils mémoire
</div>

Une fois les profils mémoire générés, ils doivent être analysés.
Pour ce faire, vous pouvez utiliser l’outil de `jemalloc` appelé [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in). Il peut être installé de plusieurs façons :

* À l’aide du gestionnaire de paquets du système
* En clonant le [dépôt jemalloc](https://github.com/jemalloc/jemalloc) et en exécutant `autogen.sh` depuis le dossier racine. Cela fournira le script `jeprof` dans le dossier `bin`

:::note
`jeprof` utilise `addr2line` pour générer des stacktraces, ce qui peut être très lent.
Si c’est le cas, il est recommandé d’installer une [implémentation alternative](https://github.com/gimli-rs/addr2line) de cet outil.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

:::

Il existe de nombreux formats pouvant être générés à partir du profil mémoire avec `jeprof`.
Il est recommandé d&#39;exécuter `jeprof --help` pour obtenir des informations sur son utilisation et les différentes options offertes par l&#39;outil.

En général, la commande `jeprof` s&#39;utilise comme suit :

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

Si vous voulez comparer les allocations survenues entre deux profils, vous pouvez définir l’argument `base` :

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

<div id="examples">
  ### Exemples
</div>

* si vous souhaitez générer un fichier texte avec une procédure par ligne :

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

* si vous souhaitez générer un fichier PDF avec un graphe d’appels :

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### Génération d’un flame graph
</div>

`jeprof` vous permet de générer des piles compactées pour créer des flame graphs.

Vous devez utiliser l’argument `--collapsed` :

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

Après cela, vous pouvez utiliser de nombreux outils pour visualiser des piles compactées.

Le plus populaire est [FlameGraph](https://github.com/brendangregg/FlameGraph), qui contient un script nommé `flamegraph.pl` :

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Un autre outil intéressant est [speedscope](https://www.speedscope.app/), qui permet d’analyser les piles d’appels collectées de façon plus interactive.

<div id="controlling-allocation-profiler-during-runtime">
  ## Contrôler le profileur d’allocation à l’exécution
</div>

Si ClickHouse/Keeper est démarré avec le profileur activé, des commandes supplémentaires permettent d’activer ou de désactiver le profilage des allocations à l’exécution.
Ces commandes facilitent le profilage sur des intervalles précis uniquement.

Pour désactiver le profileur :

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC DISABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmdp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Pour activer le profileur :

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC ENABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmep | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Il est également possible de contrôler l’état initial du profileur en définissant l’option `prof_active`, activée par défaut.
Par exemple, si vous ne souhaitez pas échantillonner les allocations au démarrage, mais seulement ensuite, vous pouvez activer le profileur. Vous pouvez démarrer ClickHouse/Keeper avec la variable d’environnement suivante :

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

Le profileur peut être activé ultérieurement.

<div id="additional-options-for-profiler">
  ## Options supplémentaires pour le profileur
</div>

`jemalloc` propose de nombreuses options liées au profileur. Elles peuvent être contrôlées en modifiant la variable d’environnement `MALLOC_CONF`.
Par exemple, l’intervalle entre les échantillons d’allocation peut être contrôlé avec `lg_prof_sample`.
Si vous souhaitez générer un profil mémoire tous les N octets, vous pouvez l’activer avec `lg_prof_interval`.

Il est recommandé de consulter la [page de référence](https://jemalloc.net/jemalloc.3.html) de `jemalloc` pour obtenir la liste complète des options.

<div id="other-resources">
  ## Autres ressources
</div>

ClickHouse/Keeper exposent des métriques liées à `jemalloc` de nombreuses façons différentes.

:::warning Avertissement
Il est important de noter qu&#39;aucune de ces métriques n&#39;est synchronisée avec les autres et que leurs valeurs peuvent diverger.
:::

<div id="system-table-asynchronous_metrics">
  ### Table système `asynchronous_metrics`
</div>

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[Référence](/fr/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### Table système `jemalloc_bins`
</div>

Contient des informations sur les allocations mémoire effectuées via l’allocateur jemalloc dans différentes classes de taille (bins), agrégées sur l’ensemble des arenas.

[Référence](/fr/operations/system-tables/jemalloc_bins)

<div id="prometheus">
  ### Prometheus
</div>

Toutes les métriques liées à `jemalloc` de `asynchronous_metrics` sont également exposées via l’endpoint Prometheus, à la fois dans ClickHouse et Keeper.

[Référence](/fr/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Commande 4LW `jmst` dans Keeper
</div>

Keeper prend en charge la commande 4LW `jmst`, qui renvoie les [statistiques de base de l’allocateur](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics) :

```sh
echo jmst | nc localhost 9181
```