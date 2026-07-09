---
description: 'Il s’agit d’une extension de la fonction de table deltaLake.'
sidebar_label: 'deltaLakeCluster'
sidebar_position: 46
slug: /sql-reference/table-functions/deltalakeCluster
title: 'deltaLakeCluster'
doc_type: 'reference'
---

Il s’agit d’une extension de la fonction de table [deltaLake](/fr/sql-reference/table-functions/deltalake.md).

Permet de traiter en parallèle les fichiers des tables [Delta Lake](https://github.com/delta-io/delta) dans Amazon S3 depuis plusieurs nœuds d’un cluster spécifié. Sur le nœud initiateur, cela établit une connexion avec tous les nœuds du cluster et répartit dynamiquement chaque fichier. Sur le nœud worker, cela demande à l’initiateur la tâche suivante à traiter, puis l’exécute. Cette opération se répète jusqu’à ce que toutes les tâches soient terminées.

<div id="syntax">
  ## Syntaxe
</div>

```sql
deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeCluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeS3Cluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
deltaLakeAzureCluster(cluster_name, named_collection[, option=value [,..]])
```

`deltaLakeS3Cluster` est un alias de `deltaLakeCluster`, tous deux concernent S3.

<div id="arguments">
  ## Arguments
</div>

* `cluster_name` — Nom d’un cluster utilisé pour constituer un ensemble d’adresses et de paramètres de connexion pour les serveurs distants et locaux.
* La description de tous les autres arguments est identique à celle des arguments de la fonction de table [deltaLake](/fr/sql-reference/table-functions/deltalake.md) équivalente.
* Un paramètre facultatif `extra_credentials` peut être utilisé pour transmettre un `role_arn` afin de mettre en place un accès basé sur les rôles dans ClickHouse Cloud. Voir [Secure S3](/fr/cloud/data-sources/secure-s3) pour les étapes de configuration.

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table dotée de la structure spécifiée, permettant de lire les données du cluster dans la table Delta Lake spécifiée sur S3.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin d’accès au fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Horodatage de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si l’horodatage est inconnu, la valeur est `NULL`.
* `_etag` — ETag du fichier. Type : `LowCardinality(String)`. Si l’ETag est inconnu, la valeur est `NULL`.

<div id="related">
  ## Voir aussi
</div>

* [moteur deltaLake](/fr/engines/table-engines/integrations/deltalake.md)
* [fonction de table deltaLake](/fr/sql-reference/table-functions/deltalake.md)