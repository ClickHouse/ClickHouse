---
description: 'Fournit une interface de type table pour `SELECT` et `INSERT` des données depuis Google
  Cloud Storage. Nécessite le rôle IAM `Storage Object User`.'
keywords: ['gcs', 'bucket']
sidebar_label: 'gcs'
sidebar_position: 70
slug: /sql-reference/table-functions/gcs
title: 'gcs'
doc_type: 'reference'
---

Fournit une interface de type table pour `SELECT` et `INSERT` des données depuis [Google Cloud Storage](https://cloud.google.com/storage/). Nécessite le [rôle IAM `Storage Object User`](https://cloud.google.com/storage/docs/access-control/iam-roles).

Il s&#39;agit d&#39;un alias de la [fonction de table s3](../../sql-reference/table-functions/s3.md).

Si votre cluster comporte plusieurs répliques, vous pouvez utiliser à la place la [fonction s3Cluster](../../sql-reference/table-functions/s3Cluster.md) (qui fonctionne avec GCS) pour paralléliser les insertions.

<div id="syntax">
  ## Syntaxe
</div>

```sql
gcs(url [, NOSIGN | hmac_key, hmac_secret] [,format] [,structure] [,compression_method])
gcs(named_collection[, option=value [,..]])
```

:::tip GCS
La fonction de table GCS s’intègre à Google Cloud Storage en utilisant l’API XML de GCS et des clés HMAC.
Consultez la [documentation Google sur l’interopérabilité](https://cloud.google.com/storage/docs/interoperability) pour plus de détails sur le point de terminaison et les clés HMAC.
:::

<div id="arguments">
  ## Arguments
</div>

| Argument                     | Description                                                                                                                                                                                                                                   |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                        | Chemin du fichier dans le bucket. Prend en charge les caractères génériques suivants en mode `readonly` : `*`, `**`, `?`, `{abc,def}` et `{N..M}`, où `N` et `M` sont des nombres, et `'abc'` et `'def'` des chaînes.                         |
| `NOSIGN`                     | Si ce mot-clé est fourni à la place des informations d&#39;authentification, aucune requête ne sera signée.                                                                                                                                   |
| `hmac_key` and `hmac_secret` | Clés qui spécifient les informations d&#39;authentification à utiliser avec le point de terminaison indiqué. Facultatif.                                                                                                                      |
| `format`                     | Le [format](/fr/sql-reference/formats) du fichier.                                                                                                                                                                                               |
| `structure`                  | Structure de la table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                  |
| `compression_method`         | Le paramètre est facultatif. Valeurs prises en charge : `none`, `gzip` ou `gz`, `brotli` ou `br`, `xz` ou `LZMA`, `zstd` ou `zst`. Par défaut, la méthode de compression est détectée automatiquement à partir de l&#39;extension du fichier. |

:::note GCS
Le chemin GCS utilise ce format, car le point de terminaison de l&#39;API XML de Google est différent de celui de l&#39;API JSON :

```text
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
```

et non ~~https://storage.cloud.google.com~~.
:::

Les arguments peuvent aussi être transmis à l’aide de [collections nommées](/fr/operations/named-collections.md). Dans ce cas, `url`, `format`, `structure`, `compression_method` fonctionnent de la même manière, et certains paramètres supplémentaires sont pris en charge :

| Paramètre                     | Description                                                                                                                                                                                                                                                  |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `access_key_id`               | `hmac_key`, facultatif.                                                                                                                                                                                                                                      |
| `secret_access_key`           | `hmac_secret`, facultatif.                                                                                                                                                                                                                                   |
| `filename`                    | Ajouté à l’URL s’il est spécifié.                                                                                                                                                                                                                            |
| `use_environment_credentials` | Activé par défaut, permet de fournir des paramètres supplémentaires à l’aide des variables d’environnement `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | Désactivé par défaut.                                                                                                                                                                                                                                        |
| `expiration_window_seconds`   | La valeur par défaut est 120.                                                                                                                                                                                                                                |

<div id="returned_value">
  ## Valeur de retour
</div>

Une table de la structure spécifiée permettant de lire ou d’écrire des données dans le fichier spécifié.

<div id="examples">
  ## Exemples
</div>

Sélection des deux premières lignes du fichier GCS `https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz`. La méthode de compression est détectée automatiquement à partir de l’extension du fichier `.gz` :

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

La même requête que ci-dessus, mais avec la méthode de compression `gzip` spécifiée explicitement au lieu de se fier à la détection automatique :

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32', 'gzip')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="usage">
  ## Utilisation
</div>

Supposons que nous ayons plusieurs fichiers avec les URI suivants sur GCS :

* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;4.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;4.csv&#39;

Comptez le nombre de lignes dans les fichiers dont le nom se termine par les chiffres 1 à 3 :

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      18 │
└─────────┘
```

Comptez le nombre total de lignes de tous les fichiers dans ces deux répertoires :

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      24 │
└─────────┘
```

:::warning
Si votre liste de fichiers contient des intervalles de nombres avec des zéros initiaux, utilisez la syntaxe avec des accolades pour chaque chiffre séparément ou utilisez `?`.
:::

Comptez le nombre total de lignes dans les fichiers nommés `file-000.csv`, `file-001.csv`, ... , `file-999.csv` :

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'name String, value UInt32');
```

```text
┌─count()─┐
│      12 │
└─────────┘
```

Insérez des données dans le fichier `test-data.csv.gz` :

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

Insérez des données dans le fichier `test-data.csv.gz` à partir de la table existante :

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

Le glob ** peut être utilisé pour parcourir les répertoires de manière récursive. Prenons l’exemple ci-dessous : il récupère récursivement tous les fichiers du répertoire `my-test-bucket-768` :

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

Le code ci-dessous récupère les données de tous les fichiers `test-data.csv.gz` présents dans n’importe quel dossier du répertoire `my-test-bucket`, de manière récursive :

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

Pour les cas d’utilisation en production, il est recommandé d’utiliser les [collections nommées](/fr/operations/named-collections.md). Voici un exemple :

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = '***',
        secret_access_key = '***';
SELECT count(*)
FROM gcs(creds, url='https://s3-object-url.csv')
```

<div id="partitioned-write">
  ## Écriture avec partitionnement
</div>

Si vous spécifiez une expression `PARTITION BY` lors de l’insertion de données dans la table `GCS`, un fichier distinct est créé pour chaque valeur de partition. Le fait de répartir les données dans des fichiers distincts permet d’améliorer l’efficacité des opérations de lecture.

**Exemples**

1. L’utilisation de l’identifiant de partition dans une clé crée des fichiers distincts :

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```

Par conséquent, les données sont écrites dans trois fichiers : `file_x.csv`, `file_y.csv` et `file_z.csv`.

2. L’utilisation de l’identifiant de partition dans un nom de bucket entraîne la création de fichiers dans différents buckets :

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```

Par conséquent, les données sont écrites dans trois fichiers situés dans des buckets différents : `my_bucket_1/file.csv`, `my_bucket_10/file.csv` et `my_bucket_20/file.csv`.

<div id="related">
  ## Voir aussi
</div>

* [fonction de table S3](s3.md)
* [moteur S3](../../engines/table-engines/integrations/s3.md)