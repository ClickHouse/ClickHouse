---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: clickhouse-benchmark
---

# clickhouse-benchmark {#clickhouse-benchmark}

Se connecte à un serveur ClickHouse et envoie à plusieurs reprises des requêtes spécifiées.

Syntaxe:

``` bash
$ echo "single query" | clickhouse-benchmark [keys]
```

ou

``` bash
$ clickhouse-benchmark [keys] <<< "single query"
```

Si vous souhaitez envoyer un ensemble de requêtes, créez un fichier texte et placez chaque requête sur la chaîne individuelle de ce fichier. Exemple:

``` sql
SELECT * FROM system.numbers LIMIT 10000000
SELECT 1
```

Ensuite, passez ce fichier à une entrée standard de `clickhouse-benchmark`.

``` bash
clickhouse-benchmark [keys] < queries_file
```

## Touches {#clickhouse-benchmark-keys}

-   `-c N`, `--concurrency=N` — Number of queries that `clickhouse-benchmark` envoie simultanément. Valeur par défaut: 1.
-   `-d N`, `--delay=N` — Interval in seconds between intermediate reports (set 0 to disable reports). Default value: 1.
-   `-h WORD`, `--host=WORD` — Server host. Default value: `localhost`. Pour l' [mode de comparaison](#clickhouse-benchmark-comparison-mode) vous pouvez utiliser plusieurs `-h` touches.
-   `-p N`, `--port=N` — Server port. Default value: 9000. For the [mode de comparaison](#clickhouse-benchmark-comparison-mode) vous pouvez utiliser plusieurs `-p` touches.
-   `-i N`, `--iterations=N` — Total number of queries. Default value: 0.
-   `-r`, `--randomize` — Random order of queries execution if there is more then one input query.
-   `-s`, `--secure` — Using TLS connection.
-   `-t N`, `--timelimit=N` — Time limit in seconds. `clickhouse-benchmark` arrête d'envoyer des requêtes lorsque le délai spécifié est atteint. Valeur par défaut: 0 (limite de temps désactivée).
-   `--confidence=N` — Level of confidence for T-test. Possible values: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99.5%). Default value: 5. In the [mode de comparaison](#clickhouse-benchmark-comparison-mode) `clickhouse-benchmark` effectue les [Test T de L'étudiant indépendant à deux échantillons](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test) tester pour déterminer si les deux distributions ne sont pas différentes avec le niveau de confiance sélectionné.
-   `--cumulative` — Printing cumulative data instead of data per interval.
-   `--database=DATABASE_NAME` — ClickHouse database name. Default value: `default`.
-   `--json=FILEPATH` — JSON output. When the key is set, `clickhouse-benchmark` génère un rapport dans le fichier JSON spécifié.
-   `--user=USERNAME` — ClickHouse user name. Default value: `default`.
-   `--password=PSWD` — ClickHouse user password. Default value: empty string.
-   `--stacktrace` — Stack traces output. When the key is set, `clickhouse-bencmark` affiche les traces d'exceptions de la pile.
-   `--stage=WORD` — Query processing stage at server. ClickHouse stops query processing and returns answer to `clickhouse-benchmark` à l'étape spécifiée. Valeurs possibles: `complete`, `fetch_columns`, `with_mergeable_state`. Valeur par défaut: `complete`.
-   `--help` — Shows the help message.

Si vous voulez appliquer des [paramètre](../../operations/settings/index.md) pour les requêtes, les passer comme une clé `--<session setting name>= SETTING_VALUE`. Exemple, `--max_memory_usage=1048576`.

## Sortie {#clickhouse-benchmark-output}

Par défaut, `clickhouse-benchmark` les rapports de chaque `--delay` intervalle.

Exemple de rapport:

``` text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%      0.145 sec.
10.000%     0.146 sec.
20.000%     0.146 sec.
30.000%     0.146 sec.
40.000%     0.147 sec.
50.000%     0.148 sec.
60.000%     0.148 sec.
70.000%     0.148 sec.
80.000%     0.149 sec.
90.000%     0.150 sec.
95.000%     0.150 sec.
99.000%     0.150 sec.
99.900%     0.150 sec.
99.990%     0.150 sec.
```

Dans le rapport, vous pouvez trouver:

-   Nombre de requêtes dans le `Queries executed:` champ.

-   Le statut de chaîne de caractères contenant (dans l'ordre):

    -   Point de terminaison du serveur ClickHouse.
    -   Nombre de requêtes traitées.
    -   QPS: QPS: combien de requêtes serveur effectuées par seconde pendant une période spécifiée dans le `--delay` argument.
    -   RPS: combien de lignes le serveur a lues par seconde pendant une période spécifiée dans `--delay` argument.
    -   MiB/ s: combien de mebibytes serveur lus par seconde pendant une période spécifiée dans le `--delay` argument.
    -   résultat RPS: combien de lignes placées par le serveur au résultat d'une requête par seconde pendant une période spécifiée dans le `--delay` argument.
    -   MiB / s de résultat. combien de mibibytes placés par le serveur au résultat d'une requête par seconde pendant une période spécifiée dans `--delay` argument.

-   Percentiles du temps d'exécution des requêtes.

## Mode De Comparaison {#clickhouse-benchmark-comparison-mode}

`clickhouse-benchmark` peut comparer les performances pour deux serveurs clickhouse en cours d'exécution.

Pour utiliser le mode de comparaison, spécifiez les points de terminaison des deux serveurs par deux paires de `--host`, `--port` touches. Clés appariées ensemble par position dans la liste des arguments, la première `--host` est assorti avec le premier `--port` et ainsi de suite. `clickhouse-benchmark` établit les connexions aux serveurs, puis envoie des requêtes. Chaque requête adressée à un serveur sélectionné au hasard. Les résultats sont présentés pour chaque serveur séparément.

## Exemple {#clickhouse-benchmark-example}

``` bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark -i 10
```

``` text
Loaded 1 queries.

Queries executed: 6.

localhost:9000, queries 6, QPS: 6.153, RPS: 123398340.957, MiB/s: 941.455, result RPS: 61532982.200, result MiB/s: 469.459.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.159 sec.
30.000%     0.160 sec.
40.000%     0.160 sec.
50.000%     0.162 sec.
60.000%     0.164 sec.
70.000%     0.165 sec.
80.000%     0.166 sec.
90.000%     0.166 sec.
95.000%     0.167 sec.
99.000%     0.167 sec.
99.900%     0.167 sec.
99.990%     0.167 sec.



Queries executed: 10.

localhost:9000, queries 10, QPS: 6.082, RPS: 121959604.568, MiB/s: 930.478, result RPS: 60815551.642, result MiB/s: 463.986.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.160 sec.
30.000%     0.163 sec.
40.000%     0.164 sec.
50.000%     0.165 sec.
60.000%     0.166 sec.
70.000%     0.166 sec.
80.000%     0.167 sec.
90.000%     0.167 sec.
95.000%     0.170 sec.
99.000%     0.172 sec.
99.900%     0.172 sec.
99.990%     0.172 sec.
```
