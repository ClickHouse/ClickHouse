---
description: "Documentation du profileur de requêtes par échantillonnage dans ClickHouse"
sidebar_label: 'Profilage des requêtes'
sidebar_position: 54
slug: /operations/optimizing-performance/sampling-query-profiler
title: "Profileur de requêtes par échantillonnage"
doc_type: 'référence'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="sampling-query-profiler">
  # profileur de requêtes par échantillonnage
</div>

ClickHouse exécute un profileur par échantillonnage qui permet d’analyser l’exécution des requêtes.
À l’aide de ce profileur, vous pouvez identifier les routines du code source les plus fréquemment utilisées pendant l’exécution des requêtes.
Vous pouvez suivre le temps CPU et le temps réel écoulé, y compris le temps d’inactivité.

Le query profiler est automatiquement activé dans ClickHouse Cloud.
L’exemple de requête suivant trouve les traces de pile les plus fréquentes pour une requête profilée, avec les noms de fonctions résolus et leurs emplacements dans le code source :

:::tip
Remplacez la valeur `query_id` par l’ID de la requête que vous souhaitez profiler.
:::

<Tabs groupId="deployment">
  <TabItem value="cloud" label="ClickHouse Cloud">
    Dans ClickHouse Cloud, vous pouvez obtenir l’ID de la requête en cliquant sur **&quot;...&quot;** tout à droite de la barre située au-dessus du tableau de résultats de la requête (à côté du bouton bascule tableau/graphique). Cela ouvre un menu contextuel dans lequel vous pouvez cliquer sur **&quot;Copy query ID&quot;**.

    Utilisez `clusterAllReplicas(default, system.trace_log)` pour interroger tous les nœuds du cluster :

    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM clusterAllReplicas(default, system.trace_log)
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>

  <TabItem value="self-managed" label="Autogéré">
    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>
</Tabs>

<div id="self-managed-query-profiler">
  ## Utilisation du profileur de requêtes dans les déploiements autogérés
</div>

Dans les déploiements autogérés, pour utiliser le profileur de requêtes, suivez les étapes ci-dessous :

<VerticalStepper headerLevel="h3">
  ### Installer ClickHouse avec les informations de débogage

  Installez le package `clickhouse-common-static-dbg` :

  1. Suivez les instructions de l’étape [« Set up the Debian repository »](/fr/install/debian_ubuntu#setup-the-debian-repository)
  2. Exécutez `sudo apt-get install clickhouse-server clickhouse-client clickhouse-common-static-dbg` pour installer les fichiers binaires compilés de ClickHouse avec les informations de débogage
  3. Exécutez `sudo service clickhouse-server start` pour démarrer le serveur
  4. Exécutez `clickhouse-client`. Les symboles de débogage de `clickhouse-common-static-dbg` seront automatiquement pris en compte par le serveur : vous n’avez rien de particulier à faire pour les activer

  ### Vérifier la configuration du serveur

  Assurez-vous que la section [`trace_log`](../../operations/server-configuration-parameters/settings.md#trace_log) de votre [fichier de configuration du serveur](/fr/operations/configuration-files) est bien configurée. Elle est activée par défaut :

  ```xml
  <!-- Journal des traces. Stocke les traces de pile collectées par les profileurs de requêtes.
       Voir les paramètres query_profiler_real_time_period_ns et query_profiler_cpu_time_period_ns. -->
  <trace_log>
      <database>system</database>
      <table>trace_log</table>

      <partition_by>toYYYYMM(event_date)</partition_by>
      <flush_interval_milliseconds>7500</flush_interval_milliseconds>
      <max_size_rows>1048576</max_size_rows>
      <reserved_size_rows>8192</reserved_size_rows>
      <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
      <!-- Indique si les journaux doivent être écrits sur le disque en cas de plantage -->
      <flush_on_crash>false</flush_on_crash>
      <symbolize>true</symbolize>
  </trace_log>
  ```

  Cette section configure la table système [trace&#95;log](/fr/operations/system-tables/trace_log), qui contient les résultats du profileur.
  N’oubliez pas que les données de cette table ne sont valides que tant que le serveur est en cours d’exécution.
  Après un redémarrage du serveur, ClickHouse ne nettoie pas la table et toutes les adresses de mémoire virtuelle stockées peuvent devenir invalides.

  ### Configurer les temporisateurs de profilage

  Configurez les paramètres [`query_profiler_cpu_time_period_ns`](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) ou [`query_profiler_real_time_period_ns`](../../operations/settings/settings.md#query_profiler_real_time_period_ns).
  Les deux paramètres peuvent être utilisés simultanément.

  Ces paramètres vous permettent de configurer les temporisateurs du profileur.
  Comme il s’agit de paramètres de session, vous pouvez définir une fréquence d’échantillonnage différente pour l’ensemble du serveur, pour des utilisateurs individuels ou des profils d’utilisateurs, pour votre session interactive et pour chaque requête.

  La fréquence d’échantillonnage par défaut est d’un échantillon par seconde, et les temporisateurs CPU comme temps réel sont activés.
  Cette fréquence permet de collecter suffisamment d’informations sur votre cluster ClickHouse sans affecter les performances de votre serveur.
  Si vous devez profiler chaque requête individuellement, utilisez une fréquence d’échantillonnage plus élevée.

  ### Analyser la table système `trace_log`

  Pour analyser la table système `trace_log`, autorisez les fonctions d’introspection avec le paramètre [`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions) :

  ```sql
  SET allow_introspection_functions=1
  ```

  :::note
  Pour des raisons de sécurité, les fonctions d’introspection sont désactivées par défaut
  :::

  Utilisez les [fonctions d’introspection](../../sql-reference/functions/introspection.md) `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` et `demangle` pour obtenir les noms des fonctions et leur position dans le code de ClickHouse.
  Pour obtenir un profil pour une requête donnée, vous devez agréger les données de la table `trace_log`.
  Vous pouvez agréger les données par fonction individuelle ou par trace de pile complète.

  :::tip
  Si vous devez visualiser les informations de `trace_log`, essayez [flamegraph](/fr/interfaces/third-party/gui#clickhouse-flamegraph) et [speedscope](https://www.speedscope.app).
  :::
</VerticalStepper>

<div id="flamegraph">
  ## Génération de flame graphs avec la fonction `flameGraph`
</div>

ClickHouse fournit la fonction d’agrégation [`flameGraph`](/fr/sql-reference/aggregate-functions/reference/flame_graph), qui génère un flame graph directement à partir des traces de pile stockées dans `trace_log`.
Le résultat est un tableau de chaînes dans un format compatible avec [flamegraph.pl](https://github.com/brendangregg/FlameGraph).

**Syntaxe :**

```sql
flameGraph(traces, [size = 1], [ptr = 0])
```

**Arguments :**

* `traces` — une stacktrace. [`Array(UInt64)`](/fr/sql-reference/data-types/array).
* `size` — une taille d’allocation pour le profilage mémoire. [`Int64`](/fr/sql-reference/data-types/int-uint).
* `ptr` — une adresse d’allocation. [`UInt64`](/fr/sql-reference/data-types/int-uint).

Lorsque `ptr` est non nul, `flameGraph` associe les allocations (`size > 0`) et les désallocations (`size < 0`) ayant la même taille et le même pointeur.
Seules les allocations qui n’ont pas été libérées sont affichées.
Les désallocations sans correspondance sont ignorées.

<div id="cpu-flame-graph">
  ### Flame graph du CPU
</div>

:::note
Les requêtes ci-dessous nécessitent l’installation de [flamegraph.pl](https://github.com/brendangregg/FlameGraph).

Pour cela, exécutez :

```bash
git clone https://github.com/brendangregg/FlameGraph
# Then use it as:
# ~/FlameGraph/flamegraph.pl
```

Remplacez `flamegraph.pl` dans les requêtes suivantes par le chemin d’accès à `flamegraph.pl` sur votre machine
:::

```sql
SET query_profiler_cpu_time_period_ns = 10000000;
```

Exécutez votre requête, puis générez le flame graph :

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(arrayReverse(trace)))
        FROM system.trace_log
        WHERE trace_type = 'CPU' AND query_id = '<query_id>'" \
    | flamegraph.pl > flame_cpu.svg
```

<div id="memory-flame-graph-all">
  ### Flame graph de la mémoire — toutes les allocations
</div>

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

Lancez votre requête, puis générez le flame graph :

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
```

<div id="memory-flame-graph-unfreed">
  ### Flame graph de la mémoire — allocations non libérées
</div>

Cette variante associe les allocations aux désallocations par pointeur et n’affiche que la mémoire qui n’a pas été libérée pendant la requête.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1,
    use_uncompressed_cache = 1,
    merge_tree_max_rows_to_use_cache = 100000000000,
    merge_tree_max_bytes_to_use_cache = 1000000000000;
```

Exécutez la requête suivante pour créer le flame graph :

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_unfreed.svg
```

<div id="memory-flame-graph-time-point">
  ### Flame graph de la mémoire — allocations actives à un instant donné
</div>

Cette approche permet d’identifier le pic d’utilisation de la mémoire et de visualiser ce qui était alloué à cet instant.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

<div id="find-memory-usage-over-time">
  #### Identifier l’utilisation de la mémoire au fil du temps
</div>

```sql
SELECT
    event_time,
    formatReadableSize(max(s)) AS m
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
)
GROUP BY event_time
ORDER BY event_time;
```

<div id="find-time-point-maximum-memory-usage">
  #### Trouvez l’instant où l’utilisation de la mémoire est maximale
</div>

```sql
SELECT
    argMax(event_time, s),
    max(s)
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
);
```

<div id="build-flame-graph">
  #### Créer un flame graph des allocations actives à ce moment précis
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time <= '<time_point>'
            ORDER BY event_time
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_pos.svg
```

<div id="build-flame-graph-deallocations">
  #### Construire un flame graph des désallocations après ce moment-là (pour comprendre ce qui a été libéré ensuite)
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, -size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time > '<time_point>'
            ORDER BY event_time DESC
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_neg.svg
```

<div id="example">
  ## Exemple
</div>

L’extrait de code ci-dessous :

* Filtre les données de `trace_log` selon un identifiant de requête et la date du jour.
* Agrège par trace de pile.
* Utilise des fonctions d’introspection pour obtenir un rapport indiquant :
  * Les noms des symboles et les fonctions correspondantes du code source.
  * Les emplacements de ces fonctions dans le code source.

```sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = '<query_id>') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```