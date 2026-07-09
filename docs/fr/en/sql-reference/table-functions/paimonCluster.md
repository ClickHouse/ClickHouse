---
description: 'Une extension de la fonction de table paimon qui permet de traiter en parallèle
  des fichiers Apache Paimon depuis plusieurs nœuds d''un cluster spécifié.'
sidebar_label: 'paimonCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/paimonCluster
title: 'paimonCluster'
doc_type: 'référence'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimoncluster-table-function">
  # paimonCluster Fonction de table
</div>

<ExperimentalBadge />

Il s&#39;agit d&#39;une extension de la [fonction de table paimon](/fr/sql-reference/table-functions/paimon.md).

Elle permet de traiter en parallèle des fichiers Apache [Paimon](https://paimon.apache.org/) sur plusieurs nœuds d&#39;un cluster spécifié. Sur l&#39;initiateur, elle établit une connexion avec tous les nœuds du cluster et répartit dynamiquement chaque fichier. Sur le nœud worker, elle demande à l&#39;initiateur la prochaine tâche à traiter, puis l&#39;exécute. Ce processus se répète jusqu&#39;à ce que toutes les tâches soient terminées.

<div id="syntax">
  ## Syntaxe
</div>

```sql
paimonS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## Arguments
</div>

* `cluster_name` — Nom d’un cluster utilisé pour construire un ensemble d’adresses et de paramètres de connexion pour les serveurs distants et locaux.
* La description de tous les autres arguments est identique à celle des arguments de la fonction de table [paimon](/fr/sql-reference/table-functions/paimon.md) équivalente.
* Un paramètre facultatif `extra_credentials` peut être utilisé pour transmettre un `role_arn` pour l’accès basé sur les rôles dans ClickHouse Cloud. Voir [Secure S3](/fr/cloud/data-sources/secure-s3) pour les étapes de configuration.

**Valeur renvoyée**

Table ayant la structure spécifiée pour lire les données du cluster à partir de la table Paimon spécifiée.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si la date et l’heure sont inconnues, la valeur est `NULL`.
* `_etag` — ETag du fichier. Type : `LowCardinality(String)`. Si l’ETag est inconnu, la valeur est `NULL`.

**Voir aussi**

* [fonction de table Paimon](/fr/sql-reference/table-functions/paimon.md)