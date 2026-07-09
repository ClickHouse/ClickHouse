---
description: 'Fournit une interface de type table en lecture seule pour les tables Apache Paimon
  stockées dans Amazon S3, Azure, HDFS ou localement.'
sidebar_label: 'paimon'
sidebar_position: 90
slug: /sql-reference/table-functions/paimon
title: 'paimon'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimon-table-function">
  # Fonction de table paimon
</div>

<ExperimentalBadge />

Fournit une interface de type table en lecture seule pour les tables Apache [Paimon](https://paimon.apache.org/) stockées dans Amazon S3, Azure, HDFS ou localement.

<div id="syntax">
  ## Syntaxe
</div>

```sql
paimon(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonS3(url [,access_key_id, secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFS(path_to_table, [,format] [,compression_method])

paimonLocal(path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## Arguments
</div>

La description de ces arguments est la même que celle des fonctions de table `s3`, `azureBlobStorage`, `HDFS` et `file`.
`format` désigne le format des fichiers de données dans la table Paimon.

Pour `paimonS3`, vous pouvez utiliser le paramètre facultatif `extra_credentials` pour transmettre un `role_arn` afin d’activer l’accès basé sur les rôles dans ClickHouse Cloud. Voir [Secure S3](/fr/cloud/data-sources/secure-s3) pour les étapes de configuration.

<div id="returned-value">
  ### Valeur retournée
</div>

Une table ayant la structure spécifiée pour lire les données de la table Paimon spécifiée.

<div id="defining-a-named-collection">
  ## Définition d’une collection nommée
</div>

Voici un exemple de configuration d’une collection nommée pour stocker l’URL et les identifiants :

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM paimonS3(paimon_conf, filename = 'test_table')
DESCRIBE paimonS3(paimon_conf, filename = 'test_table')
```

<div id="aliases">
  ## Alias
</div>

La fonction de table `paimon` est désormais un alias de `paimonS3`.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette heure est inconnue, la valeur est `NULL`.
* `_etag` — ETag du fichier. Type : `LowCardinality(String)`. Si l’ETag est inconnu, la valeur est `NULL`.

<div id="data-types-supported">
  ## Types de données pris en charge
</div>

| Type de données Paimon            | Type de données ClickHouse |
| --------------------------------- | -------------------------- |
| BOOLEAN                           | Int8                       |
| TINYINT                           | Int8                       |
| SMALLINT                          | Int16                      |
| INTEGER                           | Int32                      |
| BIGINT                            | Int64                      |
| FLOAT                             | Float32                    |
| DOUBLE                            | Float64                    |
| STRING,VARCHAR,BYTES,VARBINARY    | String                     |
| DATE                              | Date                       |
| TIME(p),TIME                      | Time(&#39;UTC&#39;)        |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | DateTime64                 |
| TIMESTAMP(p)                      | DateTime64(&#39;UTC&#39;)  |
| CHAR                              | FixedString(1)             |
| BINARY(n)                         | FixedString(n)             |
| DECIMAL(P,S)                      | Decimal(P,S)               |
| ARRAY                             | Array                      |
| MAP                               | Map                        |

<div id="partition-supported">
  ## Partition prise en charge
</div>

Types de données pris en charge pour les clés de partition Paimon :

* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`

<div id="see-also">
  ## Voir aussi
</div>

* [Fonction de table de cluster Paimon](/fr/sql-reference/table-functions/paimonCluster.md)