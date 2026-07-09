---
description: "Une extension de la fonction de table iceberg qui permet de traiter des fichiers
  Apache Iceberg en parallèle sur plusieurs nœuds d’un cluster spécifié."
sidebar_label: 'icebergCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/icebergCluster
title: 'icebergCluster'
doc_type: 'reference'
---

Il s’agit d’une extension de la fonction de table [iceberg](/fr/sql-reference/table-functions/iceberg.md).

Permet de traiter des fichiers Apache [Iceberg](https://iceberg.apache.org/) en parallèle sur plusieurs nœuds d’un cluster spécifié. Sur le nœud initiateur, elle établit une connexion à tous les nœuds du cluster et répartit dynamiquement chaque fichier. Sur le nœud worker, elle demande à l’initiateur la tâche suivante à traiter, puis l’exécute. Ce processus se répète jusqu’à ce que toutes les tâches soient terminées.

<div id="syntax">
  ## Syntaxe
</div>

```sql
icebergS3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3Cluster(cluster_name, named_collection[, option=value [,..]])

icebergAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzureCluster(cluster_name, named_collection[, option=value [,..]])

icebergHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
icebergHDFSCluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Arguments
</div>

* `cluster_name` — Nom d’un cluster servant à constituer un ensemble d’adresses et de paramètres de connexion pour les serveurs distants et locaux.
* La description de tous les autres arguments est identique à celle des arguments de la fonction de table [iceberg](/fr/sql-reference/table-functions/iceberg.md) équivalente.
* Un paramètre facultatif `extra_credentials` peut être utilisé pour transmettre un `role_arn` afin d’obtenir un accès basé sur les rôles dans ClickHouse Cloud. Consultez [Secure S3](/fr/cloud/data-sources/secure-s3) pour connaître les étapes de configuration.

**Valeur renvoyée**

Une table de la structure spécifiée permettant de lire les données du cluster dans la table Iceberg spécifiée.

**Exemples**

```sql
SELECT * FROM icebergS3Cluster('cluster_simple', 'http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette information est inconnue, la valeur est `NULL`.
* `_etag` — eTag du fichier. Type : `LowCardinality(String)`. Si l’eTag est inconnu, la valeur est `NULL`.

**Voir aussi**

* [Moteur Iceberg](/fr/engines/table-engines/integrations/iceberg.md)
* [Fonction de table Iceberg](/fr/sql-reference/table-functions/iceberg.md)