---
description: 'Crée une table à partir de fichiers stockés dans HDFS. Cette fonction de table est similaire aux fonctions de table url et file.'
sidebar_label: 'hdfs'
sidebar_position: 80
slug: /sql-reference/table-functions/hdfs
title: 'hdfs'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-function">
  # Fonction de table hdfs
</div>

Crée une table à partir de fichiers stockés dans HDFS. Cette fonction de table est similaire aux fonctions de table [url](../../sql-reference/table-functions/url.md) et [file](../../sql-reference/table-functions/file.md).

<div id="syntax">
  ## Syntaxe
</div>

```sql
hdfs(URI, format, structure)
```

<div id="arguments">
  ## Arguments
</div>

| Argument    | Description                                                                                                                                                                                                              |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `URI`       | L’URI relative du fichier dans HDFS. Le chemin du fichier prend en charge les globs suivants en mode lecture seule : `*`, `?`, `{abc,def}` et `{N..M}`, où `N` et `M` sont des nombres, et `'abc'`, `'def'` des chaînes. |
| `format`    | Le [format](/fr/sql-reference/formats) du fichier.                                                                                                                                                                          |
| `structure` | Structure de la table. Format : `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                           |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table ayant la structure spécifiée, pour lire ou écrire des données dans le fichier indiqué.

**exemple**

Table issue de `hdfs://hdfs1:9000/test` et sélection des deux premières lignes :

```sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="globs_in_path">
  ## Globs dans le chemin
</div>

Les chemins peuvent utiliser des globs. Les fichiers doivent correspondre à l’intégralité du motif du chemin, et pas seulement au suffixe ou au préfixe.

* `*` — Représente un nombre arbitraire de caractères, à l’exception de `/`, y compris la chaîne vide.
* `**` — Représente tous les fichiers d’un dossier, récursivement.
* `?` — Représente un seul caractère arbitraire.
* `{some_string,another_string,yet_another_one}` — Remplace par l’une des chaînes `'some_string', 'another_string', 'yet_another_one'`. Les chaînes peuvent contenir le symbole `/`.
* `{N..M}` — Représente n’importe quel nombre `>= N` et `<= M`.

Les constructions avec `{}` sont similaires aux fonctions de table [remote](remote.md) et [file](file.md).

**Exemple**

1. Supposons que nous ayons plusieurs fichiers avec les URI suivantes sur HDFS :

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. Exécutez une requête pour obtenir le nombre de lignes dans ces fichiers :

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. Interrogez le nombre de lignes de tous les fichiers de ces deux répertoires :

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
Si votre liste de fichiers contient des plages de nombres avec des zéros initiaux, utilisez la syntaxe avec des accolades pour chaque chiffre séparément, ou utilisez `?`.
:::

**Exemple**

Interrogez les données dans des fichiers nommés `file000`, `file001`, ... , `file999` :

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si l’heure est inconnue, la valeur est `NULL`.

<div id="hive-style-partitioning">
  ## Paramètre use_hive_partitioning
</div>

Lorsque le paramètre `use_hive_partitioning` est défini sur 1, ClickHouse détecte le partitionnement au format Hive dans le chemin (`/name=value/`) et permet d&#39;utiliser les colonnes de partition comme colonnes virtuelles dans la requête. Ces colonnes virtuelles auront les mêmes noms que dans le chemin de partitionnement.

**Exemple**

Utilisez une colonne virtuelle créée avec un partitionnement au format Hive

```sql
SELECT * FROM HDFS('hdfs://hdfs1:9000/data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="storage-settings">
  ## Paramètres de stockage
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/fr/operations/settings/settings.md#hdfs_truncate_on_insert) - permet de tronquer le fichier avant d’y insérer des données. Désactivé par défaut.
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/fr/operations/settings/settings.md#hdfs_create_new_file_on_insert) - permet de créer un nouveau fichier à chaque insertion si le format comporte un suffixe. Désactivé par défaut.
* [hdfs&#95;skip&#95;empty&#95;files](/fr/operations/settings/settings.md#hdfs_skip_empty_files) - permet d’ignorer les fichiers vides lors de la lecture. Désactivé par défaut.

<div id="related">
  ## Voir aussi
</div>

* [Colonnes virtuelles](../../engines/table-engines/index.md#table_engines-virtual_columns)