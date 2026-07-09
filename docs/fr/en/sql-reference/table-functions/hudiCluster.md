---
description: 'Une extension de la fonction de table hudi. Permet de traiter en
  parallèle les fichiers des tables Apache Hudi dans Amazon S3 à l''aide de plusieurs nœuds d''un cluster spécifié.'
sidebar_label: 'hudiCluster'
sidebar_position: 86
slug: /sql-reference/table-functions/hudiCluster
title: 'Fonction de table hudiCluster'
doc_type: 'reference'
---

Il s&#39;agit d&#39;une extension de la fonction de table [hudi](/fr/sql-reference/table-functions/hudi.md).

Permet de traiter en parallèle les fichiers des tables Apache [Hudi](https://hudi.apache.org/) dans Amazon S3 à l&#39;aide de plusieurs nœuds d&#39;un cluster spécifié. Sur le nœud initiateur, elle établit une connexion à tous les nœuds du cluster et répartit dynamiquement chaque fichier. Sur le nœud worker, elle demande à l&#39;initiateur la tâche suivante à traiter, puis l&#39;exécute. Cette opération se répète jusqu&#39;à ce que toutes les tâches soient terminées.

<div id="syntax">
  ## Syntaxe
</div>

```sql
hudiCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## Arguments
</div>

| Argument                                     | Description                                                                                                                                                                                                                                                                                                                                                                                                              |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cluster_name`                               | Nom d’un cluster utilisé pour constituer un ensemble d’adresses et de paramètres de connexion aux serveurs distants et locaux.                                                                                                                                                                                                                                                                                           |
| `url`                                        | URL du bucket contenant le chemin vers une table Hudi existante dans S3.                                                                                                                                                                                                                                                                                                                                                 |
| `aws_access_key_id`, `aws_secret_access_key` | Identifiants à long terme de l’utilisateur du compte [AWS](https://aws.amazon.com/). Vous pouvez les utiliser pour authentifier vos requêtes. Ces paramètres sont facultatifs. Si aucun identifiant n’est spécifié, ceux de la configuration ClickHouse sont utilisés. Pour plus d’informations, consultez [Using S3 for Data Storage](/fr/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | Le [format](/fr/interfaces/formats) du fichier.                                                                                                                                                                                                                                                                                                                                                                             |
| `structure`                                  | Structure de la table. Format : `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                           |
| `compression`                                | Ce paramètre est facultatif. Valeurs prises en charge : `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Par défaut, la compression est détectée automatiquement à partir de l’extension du fichier.                                                                                                                                                                                                               |
| `extra_credentials`                          | Ce paramètre est facultatif. Utilisé pour transmettre un `role_arn` pour l’accès basé sur les rôles dans ClickHouse Cloud. Consultez [Secure S3](/fr/cloud/data-sources/secure-s3) pour les étapes de configuration.                                                                                                                                                                                                        |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table ayant la structure spécifiée, permettant de lire des données d’un cluster dans la table Hudi spécifiée sur S3.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin d’accès au fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette heure est inconnue, la valeur est `NULL`.
* `_etag` — ETag du fichier. Type : `LowCardinality(String)`. Si l’ETag est inconnu, la valeur est `NULL`.

<div id="related">
  ## Voir aussi
</div>

* [moteur Hudi](/fr/engines/table-engines/integrations/hudi.md)
* [fonction de table Hudi](/fr/sql-reference/table-functions/hudi.md)