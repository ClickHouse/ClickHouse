---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: HDFS
---

# HDFS {#table_engines-hdfs}

Ce moteur fournit l'intégration avec [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) l'écosystème en permettant de gérer les données sur [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)via ClickHouse. Ce moteur est similaire
à l' [Fichier](../special/file.md#table_engines-file) et [URL](../special/url.md#table_engines-url) moteurs, mais fournit des fonctionnalités spécifiques Hadoop.

## Utilisation {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

Le `URI` paramètre est L'URI du fichier entier dans HDFS.
Le `format` paramètre spécifie l'un des formats de fichier disponibles. Effectuer
`SELECT` requêtes, le format doit être pris en charge pour l'entrée, et à effectuer
`INSERT` queries – for output. The available formats are listed in the
[Format](../../../interfaces/formats.md#formats) section.
Le chemin le cadre de `URI` peut contenir des globules. Dans ce cas, le tableau serait en lecture seule.

**Exemple:**

**1.** Configurer le `hdfs_engine_table` table:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Remplir le fichier:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Interroger les données:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Détails De Mise En Œuvre {#implementation-details}

-   Les lectures et les écritures peuvent être parallèles
-   Pas pris en charge:
    -   `ALTER` et `SELECT...SAMPLE` opérations.
    -   Index.
    -   Réplication.

**Globs dans le chemin**

Plusieurs composants de chemin peuvent avoir des globs. Pour être traité, le fichier devrait exister et correspondre au modèle de chemin entier. Liste des fichiers détermine pendant `SELECT` (pas à l' `CREATE` moment).

-   `*` — Substitutes any number of any characters except `/` y compris la chaîne vide.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

Les Constructions avec `{}` sont similaires à l' [distant](../../../sql-reference/table-functions/remote.md) table de fonction.

**Exemple**

1.  Supposons que nous ayons plusieurs fichiers au format TSV avec les URI suivants sur HDFS:

-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_3’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_3’

1.  Il y a plusieurs façons de faire une table composée des six fichiers:

<!-- -->

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Une autre façon:

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

Table se compose de tous les fichiers dans les deux répertoires (tous les fichiers doivent satisfaire le format et le schéma décrits dans la requête):

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

!!! warning "Avertissement"
    Si la liste des fichiers contient des plages de nombres avec des zéros en tête, utilisez la construction avec des accolades pour chaque chiffre séparément ou utilisez `?`.

**Exemple**

Créer une table avec des fichiers nommés `file000`, `file001`, … , `file999`:

``` sql
CREARE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

## Les Colonnes Virtuelles {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**Voir Aussi**

-   [Les colonnes virtuelles](../index.md#table_engines-virtual_columns)

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/hdfs/) <!--hide-->
