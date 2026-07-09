---
description: 'Fournit une interface de type table en lecture seule pour les tables
  Delta Lake dans Amazon S3.'
sidebar_label: 'deltaLake'
sidebar_position: 45
slug: /sql-reference/table-functions/deltalake
title: 'deltaLake'
doc_type: 'reference'
---

Fournit une interface de type table pour les tables [Delta Lake](https://github.com/delta-io/delta) dans Amazon S3, Azure Blob Storage ou un système de fichiers monté localement, avec prise en charge de la lecture et de l&#39;écriture (à partir de la v25.10)

<div id="syntax">
  ## Syntax
</div>

`deltaLake` est un alias de `deltaLakeS3`, pris en charge à des fins de compatibilité.

```sql
deltaLake(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

deltaLakeS3(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

deltaLakeLocal(path, [,format])
```

<div id="arguments">
  ## Arguments
</div>

Les arguments de cette fonction de table sont les mêmes que ceux des fonctions de table `s3`, `azureBlobStorage`, `HDFS` et `file`, respectivement.
L’argument `format` indique le format des fichiers de données de la table Delta Lake.

Le paramètre facultatif `extra_credentials` peut être utilisé pour fournir un `role_arn` pour l’accès basé sur les rôles dans ClickHouse Cloud. Consultez [Secure S3](/fr/cloud/data-sources/secure-s3) pour connaître les étapes de configuration.

<div id="returned_value">
  ## Valeur renvoyée
</div>

Renvoie une table de la structure spécifiée pour la lecture ou l’écriture de données depuis ou vers la table Delta Lake spécifiée.

<div id="examples">
  ## Exemples
</div>

<div id="reading-data">
  ### Lecture des données
</div>

Prenons une table dans un stockage S3 à l’adresse `https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/`.
Pour lire les données de la table dans ClickHouse, exécutez :

```sql title="Query"
SELECT
    URL,
    UserAgent
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/')
WHERE URL IS NOT NULL
LIMIT 2
```

```response title="Response"
┌─URL───────────────────────────────────────────────────────────────────┬─UserAgent─┐
│ http://auto.ria.ua/search/index.kz/jobinmoscow/detail/55089/hasimages │         1 │
│ http://auto.ria.ua/search/index.kz/jobinmoscow.ru/gosushi             │         1 │
└───────────────────────────────────────────────────────────────────────┴───────────┘
```

<div id="inserting-data">
  ### Insertion de données
</div>

Prenons une table dans un stockage S3 à l’emplacement `s3://ch-docs-s3-bucket/people_10k/`.
Les écritures Delta Lake sont une fonctionnalité bêta désactivée par défaut. Activez-les comme suit (`allow_delta_lake_writes` est disponible à partir de la version 26.7 ; dans les versions antérieures, utilisez `allow_experimental_delta_lake_writes`) :

```sql title="Query"
SET allow_delta_lake_writes=1
```

Ensuite, écrivez :

```sql title="Query"
INSERT INTO TABLE FUNCTION deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>') VALUES (10001, 'John', 'Smith', 'Male', 30)
```

```response title="Response"
Query id: 09069b47-89fa-4660-9e42-3d8b1dde9b17

Ok.

1 row in set. Elapsed: 3.426 sec.
```

Vous pouvez confirmer que l’insert a réussi en relisant la table :

```sql title="Query"
SELECT *
FROM deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>')
WHERE (firstname = 'John') AND (lastname = 'Smith')
```

```response title="Response"
Query id: 65032944-bed6-4d45-86b3-a71205a2b659

   ┌────id─┬─firstname─┬─lastname─┬─gender─┬─age─┐
1. │ 10001 │ John      │ Smith    │ Male   │  30 │
   └───────┴───────────┴──────────┴────────┴─────┘
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si l&#39;heure est inconnue, la valeur est `NULL`.
* `_etag` — ETag du fichier. Type : `LowCardinality(String)`. Si l&#39;ETag est inconnu, la valeur est `NULL`.

<div id="related">
  ## Voir aussi
</div>

* [Moteur DeltaLake](/fr/engines/table-engines/integrations/deltalake.md)
* [Fonction de table cluster DeltaLake](/fr/sql-reference/table-functions/deltalakeCluster.md)