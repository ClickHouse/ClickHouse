---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: fichier
---

# fichier {#file}

Crée un tableau à partir d'un fichier. Cette fonction de table est similaire à [URL](url.md) et [hdfs](hdfs.md) ceux.

``` sql
file(path, format, structure)
```

**Les paramètres d'entrée**

-   `path` — The relative path to the file from [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). Chemin d'accès à la prise en charge des fichiers suivant les globs en mode Lecture seule: `*`, `?`, `{abc,def}` et `{N..M}` où `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [format](../../interfaces/formats.md#formats) de le fichier.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Valeur renvoyée**

Une table avec la structure spécifiée pour lire ou écrire des données dans le fichier spécifié.

**Exemple**

Paramètre `user_files_path` et le contenu du fichier `test.csv`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Table de`test.csv` et la sélection des deux premières lignes de ce:

``` sql
SELECT *
FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

``` sql
-- getting the first 10 lines of a table that contains 3 columns of UInt32 type from a CSV file
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```

**Globs dans le chemin**

Plusieurs composants de chemin peuvent avoir des globs. Pour être traité, le fichier doit exister et correspondre à l'ensemble du modèle de chemin (pas seulement le suffixe ou le préfixe).

-   `*` — Substitutes any number of any characters except `/` y compris la chaîne vide.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

Les Constructions avec `{}` sont similaires à l' [fonction de table à distance](../../sql-reference/table-functions/remote.md)).

**Exemple**

1.  Supposons que nous ayons plusieurs fichiers avec les chemins relatifs suivants:

-   ‘some_dir/some_file_1’
-   ‘some_dir/some_file_2’
-   ‘some_dir/some_file_3’
-   ‘another_dir/some_file_1’
-   ‘another_dir/some_file_2’
-   ‘another_dir/some_file_3’

1.  Interroger la quantité de lignes dans ces fichiers:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

1.  Requête de la quantité de lignes dans tous les fichiers de ces deux répertoires:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "Avertissement"
    Si votre liste de fichiers contient des plages de nombres avec des zéros en tête, utilisez la construction avec des accolades pour chaque chiffre séparément ou utilisez `?`.

**Exemple**

Interroger les données des fichiers nommés `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## Les Colonnes Virtuelles {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**Voir Aussi**

-   [Les colonnes virtuelles](https://clickhouse.tech/docs/en/operations/table_engines/#table_engines-virtual_columns)

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/file/) <!--hide-->
