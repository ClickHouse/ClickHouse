---
description: 'Fournit une interface de type table en lecture seule pour les tables Apache Hudi dans Amazon
  S3.'
sidebar_label: 'hudi'
sidebar_position: 85
slug: /sql-reference/table-functions/hudi
title: 'hudi'
doc_type: 'reference'
---

Fournit une interface de type table en lecture seule pour les tables Apache [Hudi](https://hudi.apache.org/) dans Amazon S3.

<div id="syntax">
  ## Syntaxe
</div>

```sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## Arguments
</div>

| Argument                                     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                                        | URL du bucket avec le chemin vers une table Hudi existante dans S3.                                                                                                                                                                                                                                                                                                                                                                           |
| `aws_access_key_id`, `aws_secret_access_key` | Identifiants d’authentification à long terme pour l’utilisateur du compte [AWS](https://aws.amazon.com/). Vous pouvez les utiliser pour authentifier vos requêtes. Ces paramètres sont facultatifs. Si aucun identifiant n’est spécifié, ceux de la configuration ClickHouse sont utilisés. Pour plus d’informations, consultez [Using S3 for Data Storage](/fr/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | Le [format](/fr/interfaces/formats) du fichier.                                                                                                                                                                                                                                                                                                                                                                                                  |
| `structure`                                  | Structure de la table. Format : `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                |
| `compression`                                | Le paramètre est facultatif. Valeurs prises en charge : `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Par défaut, la compression est détectée automatiquement à partir de l’extension du fichier.                                                                                                                                                                                                                                    |
| `extra_credentials`                          | Le paramètre est facultatif. Utilisé pour transmettre un `role_arn` afin d’activer le contrôle d’accès basé sur les rôles dans ClickHouse Cloud. Consultez [Secure S3](/fr/cloud/data-sources/secure-s3) pour connaître les étapes de configuration.                                                                                                                                                                                             |

<div id="returned_value">
  ## Valeur de retour
</div>

Une table ayant la structure spécifiée pour lire les données de la table Hudi spécifiée dans S3.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette information est inconnue, la valeur est `NULL`.
* `_etag` — ETag du fichier. Type : `LowCardinality(String)`. Si l’ETag est inconnu, la valeur est `NULL`.

<div id="related">
  ## Voir aussi
</div>

* [Moteur Hudi](/fr/engines/table-engines/integrations/hudi.md)
* [Fonction de table cluster Hudi](/fr/sql-reference/table-functions/hudiCluster.md)