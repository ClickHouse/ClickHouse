---
description: "Fournit une interface de type table permettant de sélectionner/d’insérer des fichiers dans Azure Blob Storage. Semblable à la fonction s3."
keywords: ['azure blob storage']
sidebar_label: 'azureBlobStorage'
sidebar_position: 10
slug: /sql-reference/table-functions/azureBlobStorage
title: 'azureBlobStorage'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="azureblobstorage-table-function">
  # Fonction de table azureBlobStorage
</div>

Fournit une interface de type table permettant de sélectionner/d&#39;insérer des fichiers dans [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). Cette fonction de table est similaire à la [fonction s3](../../sql-reference/table-functions/s3.md).

<div id="syntax">
  ## Syntaxe
</div>

<Tabs>
  <TabItem value="connection_string" label="Chaîne de connexion" default>
    Les informations d’identification sont intégrées à la chaîne de connexion ; il n’est donc pas nécessaire de fournir `account_name`/`account_key` séparément :

    ```sql
    azureBlobStorage(connection_string, container_name, blobpath [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="storage_account_url" label="URL du compte de stockage">
    Nécessite `account_name` et `account_key` comme arguments distincts :

    ```sql
    azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="named_collection" label="Collection nommée">
    Voir [Collections nommées](#named-collections) ci-dessous pour la liste complète des clés prises en charge :

    ```sql
    azureBlobStorage(named_collection[, option=value [,..]])
    ```
  </TabItem>
</Tabs>

<div id="arguments">
  ## Arguments
</div>

| Argument                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection_string`              | Une chaîne de connexion contenant des identifiants intégrés (nom du compte + clé du compte ou SAS token). Avec cette forme, `account_name` et `account_key` ne doivent **pas** être fournis séparément. Voir [Configurer une chaîne de connexion](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account).                                                                     |
| `storage_account_url`            | L&#39;URL du point de terminaison du compte de stockage, par ex. `https://myaccount.blob.core.windows.net/`. Avec cette forme, vous **devez** également fournir `account_name` et `account_key`.                                                                                                                                                                                                                                                                                                                                                                                                     |
| `container_name`                 | Nom du conteneur.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `blobpath`                       | Chemin du fichier. Prend en charge les caractères génériques suivants en mode lecture seule : `*`, `**`, `?`, `{abc,def}` et `{N..M}`, où `N`, `M` sont des nombres et `'abc'`, `'def'` des chaînes.                                                                                                                                                                                                                                                                                                                                                                                                 |
| `account_name`                   | Nom du compte de stockage. **Obligatoire** lors de l&#39;utilisation de `storage_account_url` sans SAS ; ne doit **pas** être fourni lors de l&#39;utilisation de `connection_string`.                                                                                                                                                                                                                                                                                                                                                                                                               |
| `account_key`                    | Clé du compte de stockage. **Obligatoire** lors de l&#39;utilisation de `storage_account_url` sans SAS ; ne doit **pas** être fournie lors de l&#39;utilisation de `connection_string`.                                                                                                                                                                                                                                                                                                                                                                                                              |
| `format`                         | Le [format](/fr/sql-reference/formats) du fichier.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `compression`                    | Valeurs prises en charge : `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Par défaut, la compression est détectée automatiquement à partir de l&#39;extension du fichier (comme avec le paramètre `auto`).                                                                                                                                                                                                                                                                                                                                                                                   |
| `structure`                      | Structure de la table. Format : `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `partition_strategy`             | Facultatif. Valeurs prises en charge : `WILDCARD` ou `HIVE`. `WILDCARD` nécessite un `{_partition_id}` dans le chemin, qui est remplacé par la clé de partition. `HIVE` n&#39;autorise pas les caractères génériques, suppose que le chemin correspond à la racine de la table et génère des répertoires partitionnés au format Hive, avec des Snowflake IDs comme noms de fichiers et le format de fichier comme extension. La valeur par défaut est le paramètre `file_like_engine_default_partition_strategy` (`WILDCARD` avec des paramètres `compatibility` antérieurs à `26.6`, sinon `HIVE`). |
| `partition_columns_in_data_file` | Facultatif. Utilisé uniquement avec la stratégie de partition `HIVE`. Indique à ClickHouse s&#39;il doit s&#39;attendre à ce que les colonnes de partition soient écrites dans le fichier de données. Valeur par défaut : `false`.                                                                                                                                                                                                                                                                                                                                                                   |
| `extra_credentials`              | Utilisez `client_id` et `tenant_id` pour l&#39;authentification. Si `extra_credentials` est fourni, il est prioritaire sur `account_name` et `account_key`.                                                                                                                                                                                                                                                                                                                                                                                                                                          |

<div id="named-collections">
  ## Collections nommées
</div>

Les arguments peuvent également être transmis à l’aide de [collections nommées](/fr/operations/named-collections). Dans ce cas, les clés suivantes sont prises en charge :

| Clé                   | Obligatoire | Description                                                                                                        |
| --------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------ |
| `container`           | Oui         | Nom du conteneur. Correspond à l’argument positionnel `container_name`.                                            |
| `blob_path`           | Oui         | Chemin du fichier (avec caractères génériques facultatifs). Correspond à l’argument positionnel `blobpath`.        |
| `connection_string`   | Non*        | Chaîne de connexion avec identifiants intégrés. *`connection_string` ou `storage_account_url` doit être fourni.    |
| `storage_account_url` | Non*        | URL du point de terminaison du compte de stockage. *`connection_string` ou `storage_account_url` doit être fourni. |
| `account_name`        | Non         | Obligatoire lors de l’utilisation de `storage_account_url`                                                         |
| `account_key`         | Non         | Obligatoire lors de l’utilisation de `storage_account_url`                                                         |
| `format`              | Non         | Format de fichier.                                                                                                 |
| `compression`         | Non         | Type de compression.                                                                                               |
| `structure`           | Non         | Structure de la table.                                                                                             |
| `client_id`           | Non         | ID client pour l’authentification.                                                                                 |
| `tenant_id`           | Non         | ID du tenant pour l’authentification.                                                                              |

:::note
Les noms des clés des collections nommées diffèrent des noms des arguments positionnels de la fonction : `container` (et non `container_name`) et `blob_path` (et non `blobpath`).
:::

**Exemple :**

```sql
CREATE NAMED COLLECTION azure_my_data AS
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'mycontainer',
    blob_path = 'data/*.parquet',
    account_name = 'myaccount',
    account_key = 'mykey...==',
    format = 'Parquet';

SELECT *
FROM azureBlobStorage(azure_my_data)
LIMIT 5;
```

Vous pouvez également redéfinir les valeurs de la collection nommée au moment de l’exécution de la requête :

```sql
SELECT *
FROM azureBlobStorage(azure_my_data, blob_path = 'other_data/*.csv', format = 'CSVWithNames')
LIMIT 5;
```

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table de la structure spécifiée permettant de lire ou d’écrire des données dans le fichier spécifié.

<div id="examples">
  ## Exemples
</div>

<div id="reading-with-storage-account-url">
  ### Lecture avec le format `storage_account_url`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'https://myaccount.blob.core.windows.net/',
    'mycontainer',
    'data/*.parquet',
    'myaccount',
    'mykey...==',
    'Parquet'
)
LIMIT 5;
```

<div id="reading-with-connection-string">
  ### Lecture avec la syntaxe `connection_string`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'data/*.csv',
    'CSVWithNames'
)
LIMIT 5;
```

<div id="writing-with-partitions">
  ### Écriture avec partitionnement
</div>

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_{_partition_id}.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
) PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

Ensuite, lisez une partition spécifique :

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_1.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
);
```

```response
┌─column1─┬─column2─┬─column3─┐
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette information est inconnue, la valeur est `NULL`.

<div id="partitioned-write">
  ## Écriture partitionnée
</div>

<div id="partition-strategy">
  ### Stratégie de partitionnement
</div>

Prise en charge uniquement pour les requêtes `INSERT`.

`WILDCARD` : remplace le caractère générique `{_partition_id}` dans le chemin du fichier par la clé de partition réelle. Cette option est sélectionnée par défaut uniquement lorsque les paramètres `compatibility` sont antérieurs à `26.6` ; sinon, la valeur par défaut est `HIVE` (voir le paramètre `file_like_engine_default_partition_strategy`).

`HIVE` implémente le partitionnement de style Hive pour les lectures et les écritures. Il génère des fichiers au format suivant : `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

**Exemple de stratégie de partitionnement `HIVE`**

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root',
    format = 'CSVWithNames',
    compression = 'auto',
    structure = 'year UInt16, country String, id Int32',
    partition_strategy = 'hive'
) PARTITION BY (year, country)
VALUES (2020, 'Russia', 1), (2021, 'Brazil', 2);
```

```result
SELECT _path, * FROM azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root/**.csvwithnames'
)

   ┌─_path───────────────────────────────────────────────────────────────────────────┬─id─┬─year─┬─country─┐
1. │ cont/azure_table_root/year=2021/country=Brazil/7351307847391293440.csvwithnames │  2 │ 2021 │ Brazil  │
2. │ cont/azure_table_root/year=2020/country=Russia/7351307847378710528.csvwithnames │  1 │ 2020 │ Russia  │
   └─────────────────────────────────────────────────────────────────────────────────┴────┴──────┴─────────┘
```

<div id="hive-style-partitioning">
  ## Paramètre use_hive_partitioning
</div>

Il s&#39;agit d&#39;une indication permettant à ClickHouse d&#39;analyser les fichiers partitionnés au format Hive lors de la lecture. Cela n&#39;a aucun effet sur l&#39;écriture. Pour des lectures et des écritures symétriques, utilisez l&#39;argument `partition_strategy`.

Lorsque le paramètre `use_hive_partitioning` est défini sur 1, ClickHouse détecte le partitionnement de type Hive dans le chemin (`/name=value/`) et permet d&#39;utiliser les colonnes de partition comme colonnes virtuelles dans la requête. Ces colonnes virtuelles auront les mêmes noms que dans le chemin partitionné.

**Exemple**

Utiliser une colonne virtuelle créée avec le partitionnement de type Hive

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Utilisation des signatures d’accès partagé (SAS)
</div>

Une signature d’accès partagé (SAS) est un URI qui accorde un accès restreint à un conteneur Azure Storage ou à un fichier. Utilisez-la pour fournir un accès limité dans le temps aux ressources d’un compte de stockage sans partager la clé de ce compte. Plus de détails [ici](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature).

La fonction `azureBlobStorage` prend en charge les signatures d’accès partagé (SAS).

Un [jeton SAS de blob](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) contient toutes les informations nécessaires pour authentifier la requête, notamment le blob cible, les autorisations et la période de validité. Pour créer une URL de blob, ajoutez le jeton SAS au point de terminaison du service Blob. Par exemple, si le point de terminaison est `https://clickhousedocstest.blob.core.windows.net/`, la requête devient :

```sql
SELECT count()
FROM azureBlobStorage('BlobEndpoint=https://clickhousedocstest.blob.core.windows.net/;SharedAccessSignature=sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.425 sec.
```

Sinon, les utilisateurs peuvent utiliser l’[URL SAS générée du blob](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) :

```sql
SELECT count()
FROM azureBlobStorage('https://clickhousedocstest.blob.core.windows.net/?sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.153 sec.
```

<div id="related">
  ## Voir aussi
</div>

* [Moteur de table AzureBlobStorage](/fr/engines/table-engines/integrations/azureBlobStorage.md)