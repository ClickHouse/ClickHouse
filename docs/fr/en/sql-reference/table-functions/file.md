---
description: 'Un moteur de table qui fournit une interface de type table pour exécuter des SELECT sur
  des fichiers et y faire des INSERT, à l''instar de la fonction de table s3. Utilisez `file` pour les
  fichiers locaux, et `s3` pour les buckets du stockage objet, comme S3, GCS ou MinIO.'
sidebar_label: 'file'
sidebar_position: 60
slug: /sql-reference/table-functions/file
title: 'file'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="file-table-function">
  # Fonction de table file
</div>

Un moteur de table qui fournit une interface de type table pour lire des fichiers avec `SELECT` et y écrire avec `INSERT`, à l’instar de la fonction de table [s3](/fr/sql-reference/table-functions/s3.md). Utilisez `file` lorsque vous travaillez avec des fichiers locaux, et `s3` lorsque vous travaillez avec des buckets dans un stockage objet comme S3, GCS ou MinIO.

La fonction `file` peut être utilisée dans des requêtes `SELECT` et `INSERT` pour lire des fichiers ou y écrire.

<div id="syntax">
  ## Syntaxe
</div>

```sql
file([path_to_archive ::] path [,format] [,structure] [,compression])
```

Pour les requêtes `SELECT`, `path` peut aussi être une expression qui renvoie un `Array(String)` :

```sql
file(['file1.csv', 'file2.csv'], 'CSV', 'column1 UInt32, column2 UInt32')
```

<div id="arguments">
  ## Arguments
</div>

| Parameter         | Description                                                                                                                                                                                                                                                                                                                                                                                        |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`            | Chemin relatif vers le fichier depuis [user&#95;files&#95;path](/fr/operations/server-configuration-parameters/settings.md#user_files_path), ou `Array(String)` de chemins dans les requêtes `SELECT`. En mode lecture seule, les [globs](#globs-in-path) suivants sont pris en charge : `*`, `?`, `{abc,def}` (où `'abc'` et `'def'` sont des chaînes) et `{N..M}` (où `N` et `M` sont des nombres). |
| `path_to_archive` | Chemin relatif vers une archive zip/tar/7z. Prend en charge les mêmes globs que `path`.                                                                                                                                                                                                                                                                                                            |
| `format`          | Le [format](/fr/interfaces/formats) du fichier.                                                                                                                                                                                                                                                                                                                                                       |
| `structure`       | Structure de la table. Format : `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                     |
| `compression`     | Type de compression existant lorsqu&#39;il est utilisé dans une requête `SELECT`, ou type de compression souhaité lorsqu&#39;il est utilisé dans une requête `INSERT`. Les types de compression pris en charge sont `gz`, `br`, `xz`, `zst`, `lz4` et `bz2`.                                                                                                                                       |

:::tip
Lorsque l&#39;argument `structure` est omis, ClickHouse déduit le schéma à partir du format lui-même.
Selon le format, les noms et types de colonnes par défaut diffèrent.
Pour afficher le schéma d&#39;un format donné, utilisez [`DESC`](/fr/sql-reference/statements/describe-table) avec la fonction de table [`format`](/fr/sql-reference/table-functions/format).

Par exemple :

```sql
DESC format(LineAsString, 'Hello\nWorld')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

:::

<div id="returned_value">
  ## Valeur de retour
</div>

Une table permettant de lire ou d’écrire des données dans un fichier.

<div id="examples-for-writing-to-a-file">
  ## Exemples d’écriture dans un fichier
</div>

<div id="write-to-a-tsv-file">
  ### Écrire dans un fichier TSV
</div>

```sql
INSERT INTO TABLE FUNCTION
file('test.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

Par conséquent, les données sont alors écrites dans le fichier `test.tsv` :

```bash
# cat /var/lib/clickhouse/user_files/test.tsv
1    2    3
3    2    1
1    3    2
```

<div id="partitioned-write-to-multiple-tsv-files">
  ### Écriture partitionnée dans plusieurs fichiers TSV
</div>

Si vous spécifiez une expression `PARTITION BY` lors de l’insertion de données dans une fonction de table de type `file`, un fichier distinct est créé pour chaque partition. Le découpage des données en fichiers distincts contribue à améliorer les performances des opérations de lecture.

```sql
INSERT INTO TABLE FUNCTION
file('test_{_partition_id}.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

Par conséquent, les données sont écrites dans trois fichiers : `test_1.tsv`, `test_2.tsv` et `test_3.tsv`.

```bash
# cat /var/lib/clickhouse/user_files/test_1.tsv
3    2    1

# cat /var/lib/clickhouse/user_files/test_2.tsv
1    3    2

# cat /var/lib/clickhouse/user_files/test_3.tsv
1    2    3
```

<div id="examples-for-reading-from-a-file">
  ## Exemples de lecture depuis un fichier
</div>

<div id="select-from-a-csv-file">
  ### SELECT depuis un fichier CSV
</div>

Commencez par définir `user_files_path` dans la configuration du serveur, puis préparez un fichier `test.csv` :

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Ensuite, importez les données de `test.csv` dans une table et sélectionnez-en les deux premières lignes :

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="inserting-data-from-a-file-into-a-table">
  ### Insertion de données depuis un fichier dans une table
</div>

```sql
INSERT INTO FUNCTION
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1);
```

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

Lecture des données à partir de `table.csv`, situé dans `archive1.zip` ou dans `archive2.zip`, ou dans les deux :

```sql
SELECT * FROM file('user_files/archives/archive{1..2}.zip :: table.csv');
```

<div id="globs-in-path">
  ## Globs dans le chemin
</div>

Les chemins peuvent utiliser des globs. Les fichiers doivent correspondre au motif de chemin dans son intégralité, et pas seulement au suffixe ou au préfixe. Il existe une exception : si le chemin fait référence à un répertoire existant
et n’utilise pas de globs, un `*` est implicitement ajouté au chemin afin que
tous les fichiers du répertoire soient sélectionnés.

* `*` — Représente un nombre arbitraire de caractères, sauf `/`, y compris la chaîne vide.
* `?` — Représente un caractère unique arbitraire.
* `{some_string,another_string,yet_another_one}` — Est remplacé par l’une des chaînes `'some_string', 'another_string', 'yet_another_one'`. Les chaînes peuvent contenir le symbole `/`.
* `{N..M}` — Représente tout nombre `>= N` et `<= M`.
* `**` - Représente tous les fichiers d’un dossier, récursivement.

Les constructions avec `{}` sont similaires aux fonctions de table [remote](remote.md) et [hdfs](hdfs.md).

<div id="examples">
  ## Exemples
</div>

**Exemple**

Supposons que les fichiers suivants existent avec les chemins relatifs ci-dessous :

* `some_dir/some_file_1`
* `some_dir/some_file_2`
* `some_dir/some_file_3`
* `another_dir/some_file_1`
* `another_dir/some_file_2`
* `another_dir/some_file_3`

Exécutez une requête pour obtenir le nombre total de lignes de tous les fichiers :

```sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

Une autre expression de chemin permettant d’obtenir le même résultat :

```sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

Obtenez le nombre total de lignes dans `some_dir` à l’aide du `*` implicite :

```sql
SELECT count(*) FROM file('some_dir', 'TSV', 'name String, value UInt32');
```

:::note
Si votre liste de fichiers contient des plages de nombres avec des zéros non significatifs, utilisez la construction avec des accolades pour chaque chiffre séparément ou utilisez `?`.
:::

**Exemple**

Interrogez le nombre total de lignes des fichiers nommés `file000`, `file001`, ... , `file999` :

```sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

**Exemple**

Obtenez le nombre total de lignes de tous les fichiers du répertoire `big_dir/`, récursivement :

```sql
SELECT count(*) FROM file('big_dir/**', 'CSV', 'name String, value UInt32');
```

**Exemple**

Obtenez le nombre total de lignes dans tous les fichiers `file002` de n’importe quel dossier du répertoire `big_dir/`, récursivement :

```sql
SELECT count(*) FROM file('big_dir/**/file002', 'CSV', 'name String, value UInt32');
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin d’accès au fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette date et heure sont inconnues, la valeur est `NULL`.

<div id="hive-style-partitioning">
  ## Paramètre use_hive_partitioning
</div>

Lorsque le paramètre `use_hive_partitioning` est défini sur 1, ClickHouse détecte le partitionnement de type Hive dans le chemin (`/name=value/`) et permet d’utiliser les colonnes de partition comme colonnes virtuelles dans la requête. Ces colonnes virtuelles auront les mêmes noms que dans le chemin partitionné.

**Exemple**

Utilisation d’une colonne virtuelle créée avec le partitionnement de type Hive

```sql
SELECT * FROM file('data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="settings">
  ## Paramètres
</div>

| Paramètre                                                                                                                               | Description                                                                                                                                                                                                             |
| --------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/fr/operations/settings/settings#engine_file_empty_if_not_exists)                    | permet de lire des données vides à partir d&#39;un fichier inexistant. Désactivé par défaut.                                                                                                                            |
| [engine&#95;file&#95;truncate&#95;on&#95;insert](/fr/operations/settings/settings#engine_file_truncate_on_insert)                          | permet de tronquer le fichier avant d&#39;y insérer des données. Désactivé par défaut.                                                                                                                                  |
| [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/fr/operations/settings/settings.md#engine_file_allow_create_multiple_files) | permet de créer un nouveau fichier à chaque insertion si le format comporte un suffixe. Désactivé par défaut.                                                                                                           |
| [engine&#95;file&#95;skip&#95;empty&#95;files](/fr/operations/settings/settings.md#engine_file_skip_empty_files)                           | permet d&#39;ignorer les fichiers vides lors de la lecture. Désactivé par défaut.                                                                                                                                       |
| [storage&#95;file&#95;read&#95;method](/fr/operations/settings/settings#engine_file_empty_if_not_exists)                                   | méthode de lecture des données depuis le fichier de stockage, à choisir parmi : read, pread, mmap (uniquement pour clickhouse-local). Valeur par défaut : `pread` pour clickhouse-server, `mmap` pour clickhouse-local. |

<div id="related">
  ## Voir aussi
</div>

* [Colonnes virtuelles](/fr/engines/table-engines/index.md#table_engines-virtual_columns)
* [Renommer les fichiers après traitement](/fr/operations/settings/settings.md#rename_files_after_processing)