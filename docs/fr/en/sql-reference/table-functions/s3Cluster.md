---
description: 'Une extension de la fonction de table s3, qui permet de traiter des fichiers
  depuis Amazon S3 et Google Cloud Storage en parallèle sur de nombreux nœuds d’un
  cluster spécifié.'
sidebar_label: 's3Cluster'
sidebar_position: 181
slug: /sql-reference/table-functions/s3Cluster
title: 's3Cluster'
doc_type: 'reference'
---

Il s’agit d’une extension de la fonction de table [s3](/fr/sql-reference/table-functions/s3.md).

Elle permet de traiter des fichiers depuis [Amazon S3](https://aws.amazon.com/s3/) et Google Cloud Storage [Google Cloud Storage](https://cloud.google.com/storage/) en parallèle sur de nombreux nœuds d’un cluster spécifié. Sur l’initiateur, elle établit une connexion avec tous les nœuds du cluster, développe les astérisques dans le chemin de fichier S3 et répartit dynamiquement chaque fichier. Sur le nœud worker, elle interroge l’initiateur pour obtenir la tâche suivante à traiter, puis la traite. Ce processus se répète jusqu’à ce que toutes les tâches soient terminées.

<div id="syntax">
  ## Syntaxe
</div>

```sql
s3Cluster(cluster_name, url[, NOSIGN | access_key_id, secret_access_key,[session_token]][, format][, structure][, compression_method][, headers][, extra_credentials])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Arguments
</div>

| Argument                                | Description                                                                                                                                                                                                                                                                                                                                                  |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cluster_name`                          | Nom d’un cluster utilisé pour construire un ensemble d’adresses et de paramètres de connexion pour les serveurs distants et locaux.                                                                                                                                                                                                                          |
| `url`                                   | Chemin vers un fichier ou un ensemble de fichiers. Prend en charge les caractères génériques suivants en mode `readonly` : `*`, `**`, `?`, `{'abc','def'}` et `{N..M}`, où `N` et `M` sont des nombres, et `abc` et `def` des chaînes. Pour plus d’informations, voir [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `NOSIGN`                                | Si ce mot-clé est fourni à la place des informations d’identification, aucune requête ne sera signée.                                                                                                                                                                                                                                                        |
| `access_key_id` and `secret_access_key` | Clés qui spécifient les informations d’identification à utiliser avec l’endpoint donné. Facultatif.                                                                                                                                                                                                                                                          |
| `session_token`                         | Jeton de session à utiliser avec les clés fournies. Facultatif lors du passage des clés.                                                                                                                                                                                                                                                                     |
| `format`                                | Le [format](/fr/sql-reference/formats) du fichier.                                                                                                                                                                                                                                                                                                              |
| `structure`                             | Structure de la table. Format : `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                               |
| `compression_method`                    | Ce paramètre est facultatif. Valeurs prises en charge : `none`, `gzip` ou `gz`, `brotli` ou `br`, `xz` ou `LZMA`, `zstd` ou `zst`. Par défaut, la méthode de compression est détectée automatiquement à partir de l’extension du fichier.                                                                                                                    |
| `headers`                               | Ce paramètre est facultatif. Permet de transmettre des headers dans la requête S3. Utilisez le format `headers(key=value)`, par ex. `headers('x-amz-request-payer' = 'requester')`. Voir [here](/fr/sql-reference/table-functions/s3#accessing-requester-pays-buckets) pour un exemple d’utilisation.                                                           |
| `extra_credentials`                     | Facultatif. `roleARN` peut être transmis via ce paramètre. Voir [here](/fr/cloud/data-sources/secure-s3#access-your-s3-bucket-with-the-clickhouseaccess-role) pour un exemple.                                                                                                                                                                                  |

Les arguments peuvent également être passés à l’aide de [collections nommées](/fr/operations/named-collections.md). Dans ce cas, `url`, `access_key_id`, `secret_access_key`, `format`, `structure`, `compression_method` fonctionnent de la même manière, et certains paramètres supplémentaires sont pris en charge :

| Argument                      | Description                                                                                                                                                                                                                                                      |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `filename`                    | Ajouté à l’URL s’il est spécifié.                                                                                                                                                                                                                                |
| `use_environment_credentials` | Activé par défaut, permet de transmettre des paramètres supplémentaires à l’aide des variables d’environnement `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | Désactivé par défaut.                                                                                                                                                                                                                                            |
| `expiration_window_seconds`   | La valeur par défaut est 120.                                                                                                                                                                                                                                    |

<div id="returned_value">
  ## Valeur retournée
</div>

Une table dotée de la structure spécifiée, pour la lecture ou l’écriture de données dans le fichier spécifié.

<div id="examples">
  ## Exemples
</div>

Sélectionnez les données de tous les fichiers contenus dans les dossiers `/root/data/clickhouse` et `/root/data/database/`, en utilisant tous les nœuds du cluster `cluster_simple` :

```sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'ClickHouse_Minio_P@ssw0rd',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

Comptez le nombre total de lignes de tous les fichiers du cluster `cluster_simple` :

:::tip
Si votre liste de fichiers contient des plages de nombres avec des zéros initiaux, utilisez la syntaxe avec des accolades pour chaque chiffre séparément ou utilisez `?`.
:::

Pour une utilisation en production, il est recommandé d&#39;utiliser des [collections nommées](/fr/operations/named-collections.md). Voici un exemple :

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'ClickHouse_Minio_P@ssw0rd';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

<div id="accessing-private-and-public-buckets">
  ## Accès aux buckets privés et publics
</div>

Les utilisateurs peuvent utiliser les mêmes approches que celles documentées pour la fonction s3 [ici](/fr/sql-reference/table-functions/s3#accessing-public-buckets).

<div id="optimizing-performance">
  ## Optimisation des performances
</div>

Pour en savoir plus sur l’optimisation des performances de la fonction s3, consultez [notre guide détaillé](/fr/integrations/s3/performance).

<div id="related">
  ## Voir aussi
</div>

* [Moteur S3](../../engines/table-engines/integrations/s3.md)
* [fonction de table S3](../../sql-reference/table-functions/s3.md)