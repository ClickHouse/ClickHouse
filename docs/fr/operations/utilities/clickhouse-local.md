---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: clickhouse-local
---

# clickhouse-local {#clickhouse-local}

Le `clickhouse-local` programme vous permet d'effectuer un traitement rapide sur les fichiers locaux, sans avoir à déployer et configurer le serveur ClickHouse.

Accepte les données qui représentent des tables et les interroge en utilisant [Clickhouse dialecte SQL](../../sql-reference/index.md).

`clickhouse-local` utilise le même noyau que clickhouse server, de sorte qu'il prend en charge la plupart des fonctionnalités et le même ensemble de formats et de moteurs de table.

Par défaut `clickhouse-local` n'a pas accès aux données sur le même hôte, mais il prend en charge le chargement de la configuration du serveur à l'aide `--config-file` argument.

!!! warning "Avertissement"
    Il n'est pas recommandé de charger la configuration du serveur de production dans `clickhouse-local` parce que les données peuvent être endommagées en cas d'erreur humaine.

## Utilisation {#usage}

Utilisation de base:

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

Argument:

-   `-S`, `--structure` — table structure for input data.
-   `-if`, `--input-format` — input format, `TSV` par défaut.
-   `-f`, `--file` — path to data, `stdin` par défaut.
-   `-q` `--query` — queries to execute with `;` comme délimiteur.
-   `-N`, `--table` — table name where to put output data, `table` par défaut.
-   `-of`, `--format`, `--output-format` — output format, `TSV` par défaut.
-   `--stacktrace` — whether to dump debug output in case of exception.
-   `--verbose` — more details on query execution.
-   `-s` — disables `stderr` journalisation.
-   `--config-file` — path to configuration file in same format as for ClickHouse server, by default the configuration empty.
-   `--help` — arguments references for `clickhouse-local`.

Il existe également des arguments pour chaque variable de configuration de ClickHouse qui sont plus couramment utilisés à la place de `--config-file`.

## Exemple {#examples}

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

Exemple précédent est le même que:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

Maintenant, nous allons sortie utilisateur de mémoire pour chaque utilisateur Unix:

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' | clickhouse-local -S "user String, mem Float64" -q "SELECT user, round(sum(mem), 2) as memTotal FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

``` text
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

[Article Original](https://clickhouse.tech/docs/en/operations/utils/clickhouse-local/) <!--hide-->
