---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: hdfs
---

# hdfs {#hdfs}

Crée une table à partir de fichiers dans HDFS. Cette fonction de table est similaire à [URL](url.md) et [fichier](file.md) ceux.

``` sql
hdfs(URI, format, structure)
```

**Les paramètres d'entrée**

-   `URI` — The relative URI to the file in HDFS. Path to file support following globs in readonly mode: `*`, `?`, `{abc,def}` et `{N..M}` où `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [format](../../interfaces/formats.md#formats) de le fichier.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Valeur renvoyée**

Une table avec la structure spécifiée pour lire ou écrire des données dans le fichier spécifié.

**Exemple**

Table de `hdfs://hdfs1:9000/test` et la sélection des deux premières lignes de ce:

``` sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

**Globs dans le chemin**

Plusieurs composants de chemin peuvent avoir des globs. Pour être traité, le fichier doit exister et correspondre à l'ensemble du modèle de chemin (pas seulement le suffixe ou le préfixe).

-   `*` — Substitutes any number of any characters except `/` y compris la chaîne vide.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

Les Constructions avec `{}` sont similaires à l' [fonction de table à distance](../../sql-reference/table-functions/remote.md)).

**Exemple**

1.  Supposons que nous ayons plusieurs fichiers avec les URI suivants sur HDFS:

-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_3’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_3’

1.  Interroger la quantité de lignes dans ces fichiers:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

1.  Requête de la quantité de lignes dans tous les fichiers de ces deux répertoires:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "Avertissement"
    Si votre liste de fichiers contient des plages de nombres avec des zéros en tête, utilisez la construction avec des accolades pour chaque chiffre séparément ou utilisez `?`.

**Exemple**

Interroger les données des fichiers nommés `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## Les Colonnes Virtuelles {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**Voir Aussi**

-   [Les colonnes virtuelles](https://clickhouse.tech/docs/en/operations/table_engines/#table_engines-virtual_columns)

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/hdfs/) <!--hide-->
