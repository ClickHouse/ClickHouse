---
description: 'Documentation pour highlight-next-line'
sidebar_label: 'Disques externes pour le stockage des données'
sidebar_position: 68
slug: /operations/storing-data
title: 'Disques externes pour le stockage des données'
doc_type: 'guide'
---

Les données traitées dans ClickHouse sont généralement stockées dans le système de fichiers local de la
machine sur laquelle le serveur ClickHouse s’exécute. Cela nécessite des disques de grande capacité,
qui peuvent être coûteux. Pour éviter de stocker les données localement, plusieurs options de stockage sont prises en charge :

1. Le stockage d’objets [Amazon S3](https://aws.amazon.com/s3/).
2. [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).
3. Non pris en charge : le Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html))

<br />

:::note
ClickHouse prend également en charge les table engines externes, qui sont différents de
l’option de stockage externe décrite sur cette page, car ils permettent de lire des données
stockées dans un format de fichier générique (comme Parquet). Sur cette page, nous décrivons
la configuration du stockage pour les tables de la famille `MergeTree` ou de la famille `Log`.

1. pour travailler avec des données stockées sur des disques `Amazon S3`, utilisez le table engine [S3](/fr/engines/table-engines/integrations/s3.md).
2. pour travailler avec des données stockées dans Azure Blob Storage, utilisez le table engine [AzureBlobStorage](/fr/engines/table-engines/integrations/azureBlobStorage.md).
3. pour travailler avec des données stockées dans le Hadoop Distributed File System (non pris en charge), utilisez le table engine [HDFS](/fr/engines/table-engines/integrations/hdfs.md).
   :::

<div id="configuring-external-storage">
  ## Configurer le stockage externe
</div>

Les moteurs de table des familles [`MergeTree`](/fr/engines/table-engines/mergetree-family/mergetree.md) et [`Log`](/fr/engines/table-engines/log-family/log.md)
peuvent stocker des données dans `S3`, `AzureBlobStorage` et `HDFS` (non pris en charge) à l’aide d’un disque de type `s3`,
`azure_blob_storage` ou `hdfs` (non pris en charge), respectivement.

La configuration du disque requiert :

1. Une section `type`, égale à l’une des valeurs suivantes : `s3`, `azure_blob_storage`, `hdfs` (non pris en charge), `local_blob_storage`, `web`.
2. La configuration d’un type de stockage externe spécifique.

À partir de la version 24.1 de ClickHouse, il est possible d’utiliser une nouvelle option de configuration.
Elle nécessite de spécifier :

1. Un `type` égal à `object_storage`
2. `object_storage_type`, égal à l’une des valeurs suivantes : `s3`, `azure_blob_storage` (ou simplement `azure` à partir de `24.3`), `hdfs` (non pris en charge), `local_blob_storage` (ou simplement `local` à partir de `24.3`), `web`.

<br />

Il est également possible de spécifier `metadata_type` (sa valeur par défaut est `local`) ; il peut aussi être défini sur `plain`, `web` et, à partir de `24.4`, `plain_rewritable`.
L’utilisation du type de métadonnées `plain` est décrite dans la [section stockage simple](/fr/operations/storing-data#plain-storage) ; le type de métadonnées `web` ne peut être utilisé qu’avec le type de stockage d’objets `web` ; le type de métadonnées `local` stocke les fichiers de métadonnées localement (chaque fichier de métadonnées contient la correspondance avec les fichiers du stockage d’objets, ainsi que des métadonnées supplémentaires les concernant).

Par exemple :

```xml
<s3>
    <type>s3</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

correspond à la configuration suivante (à partir de la version `24.1`) :

```xml
<s3>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>local</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

La configuration suivante :

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

est égal à :

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

Voici un exemple de configuration complète du stockage :

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

À partir de la version 24.1, cela peut aussi se présenter ainsi :

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>local</metadata_type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

Pour définir un type de stockage spécifique comme option par défaut pour toutes les tables `MergeTree`,
ajoutez la section suivante au fichier de configuration :

```xml
<clickhouse>
    <merge_tree>
        <storage_policy>s3</storage_policy>
    </merge_tree>
</clickhouse>
```

Si vous souhaitez configurer une politique de stockage spécifique pour une table donnée,
vous pouvez la définir dans les paramètres lors de la création de la table :

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3';
```

Vous pouvez également utiliser `disk` à la place de `storage_policy`. Dans ce cas, il n&#39;est pas nécessaire
d&#39;avoir de section `storage_policy` dans le fichier de configuration : une section `disk`
suffit.

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = 's3';
```

<div id="refresh-parts-interval-and-table-disk">
  ## refresh_parts_interval et table_disk
</div>

Ce paramètre est destiné aux tables MergeTree non répliquées, dans lesquelles des parties de données peuvent être écrites en externe et où la découverte des métadonnées doit être réactualisée à partir du stockage.

Le paramètre MergeTree `refresh_parts_interval` active la réactualisation périodique de la liste des parties de données à partir du stockage sous-jacent (par exemple pour prendre en compte des parties écrites en externe). La distinction essentielle se fait entre **les métadonnées partagées entre les répliques** et **les métadonnées locales à chaque réplique** (par exemple S3 avec des métadonnées locales par réplique) : ce n’est que lorsque les métadonnées sont partagées que les nouvelles parties seront visibles par toutes les répliques. Le simple usage du stockage d’objets n’implique pas des métadonnées partagées.

* **Le stockage d’objets (par exemple `disk = 's3'`) n’implique pas des métadonnées partagées.** Lorsque les métadonnées sont stockées localement pour chaque réplique (par défaut), chaque réplique gère indépendamment ses pointeurs vers les blobs dans le stockage d’objets. Les modifications effectuées sur une réplique ne sont pas visibles par les autres. Dans ce cas, `refresh_parts_interval` ne rend pas les nouvelles parties visibles entre les répliques, car les métadonnées lues par chaque réplique sont locales à celle-ci.

* **La réactualisation automatique des parties nécessite que les métadonnées du système de fichiers soient partagées** (ou que la table utilise des métadonnées en lecture seule appartenant à la table, afin que la réactualisation puisse s’appliquer). Définir `table_disk = true` avec un disk local à la table (par exemple `SETTINGS disk = disk(type=object_storage, ...), table_disk = true`) est une façon d’obtenir la sémantique correcte : la table maîtrise le cycle de vie des métadonnées et le stockage est traité comme étant en lecture seule ; ainsi, `refresh_parts_interval` s’exécute et les parties ajoutées en externe peuvent être découvertes.

* **Avec un disk défini globalement** (par exemple `disk = 's3'` dans `storage_configuration`) et les métadonnées locales par défaut, chaque réplique possède son propre état de métadonnées. Même si les blobs se trouvent dans S3, le stockage n’est pas considéré comme partagé dans le cadre de `refresh_parts_interval`, et les nouvelles parties créées en dehors de ClickHouse ou sur une autre réplique ne seront pas détectées.

Pour que la réactualisation automatique des parties fonctionne, assurez-vous que les métadonnées sont partagées, ou utilisez un disk au niveau de la table avec `table_disk = true` comme ci-dessus. Le fait de s’appuyer uniquement sur `refresh_parts_interval` avec des métadonnées locales à la réplique ne réactualisera pas les parties comme prévu.

:::note
`refresh_parts_interval` n’est pas utilisé pour les tables ReplicatedMergeTree.
Les tables répliquées synchronisent déjà les parties via le mécanisme de réplication.
Ce paramètre s’applique uniquement aux tables MergeTree non répliquées où des parties sont écrites en externe et où une réactualisation des métadonnées est nécessaire.
:::

<div id="dynamic-configuration">
  ## Configuration dynamique
</div>

Il est également possible de spécifier une configuration de stockage sans
disque prédéfini dans un fichier de configuration, en la configurant dans les
paramètres de requête `CREATE`/`ATTACH`.

La requête d’exemple suivante s’appuie sur la configuration dynamique des disques ci-dessus et
montre comment utiliser un disque local pour mettre en cache les données d’une table stockée à une URL.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=web,
    endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
  );
  -- highlight-end
```

L’exemple ci-dessous ajoute un cache au stockage externe.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
-- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
-- highlight-end
```

Dans les paramètres mis en évidence ci-dessous, notez que le disque de `type=web` est imbriqué dans
le disque de `type=cache`.

:::note
L&#39;exemple utilise `type=web`, mais n&#39;importe quel type de disque peut être configuré comme dynamique,
y compris un disque local. Les disques locaux nécessitent qu&#39;un argument `path` se trouve dans le
paramètre de configuration du serveur `custom_local_disks_base_directory`, qui n&#39;a pas de
valeur par défaut ; définissez-le donc également lorsque vous utilisez un disque local.
:::

Une combinaison de configuration basée sur le fichier de configuration et de configuration définie en SQL est
également possible :

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  -- highlight-end
```

où `web` est issu du fichier de configuration du serveur :

```xml
<storage_configuration>
    <disks>
        <web>
            <type>web</type>
            <endpoint>'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'</endpoint>
        </web>
    </disks>
</storage_configuration>
```

<div id="s3-storage">
  ### Utiliser le stockage S3
</div>

<div id="required-parameters-s3">
  #### Paramètres requis
</div>

| Paramètre           | Description                                                                                                                                                                                                       |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint`          | URL de l’endpoint S3 au format `path` ou `virtual hosted` [styles](https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html). Elle doit inclure le bucket et le chemin racine de stockage des données. |
| `access_key_id`     | ID de la clé d’accès S3 utilisée pour l’authentification.                                                                                                                                                         |
| `secret_access_key` | Clé d’accès secrète S3 utilisée pour l’authentification.                                                                                                                                                          |

<div id="optional-parameters-s3">
  #### Paramètres facultatifs
</div>

| Paramètre                                                                                                                 | Description                                                                                                                                                                                                                                                                                                                                                                                           | Valeur par défaut                                  |
| ------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| `region`                                                                                                                  | Nom de la région S3.                                                                                                                                                                                                                                                                                                                                                                                  | *                                                  |
| `support_batch_delete`                                                                                                    | Détermine s’il faut vérifier la prise en charge de la suppression par lot. Définissez cette valeur sur `false` lors de l’utilisation de Google Cloud Storage (GCS), car GCS ne prend pas en charge la suppression par lot.                                                                                                                                                                            | `true`                                             |
| `use_environment_credentials`                                                                                             | Lit les identifiants AWS à partir des variables d’environnement : `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` et `AWS_SESSION_TOKEN`, si elles existent. Remarque : les identifiants d’environnement sont partagés entre tous les disques S3. Pour utiliser des identifiants différents selon les disques, spécifiez plutôt explicitement `access_key_id` et `secret_access_key` pour chaque disque. | `false`                                            |
| `use_insecure_imds_request`                                                                                               | Si `true`, utilise une requête IMDS non sécurisée pour obtenir les identifiants à partir des métadonnées Amazon EC2.                                                                                                                                                                                                                                                                                  | `false`                                            |
| `expiration_window_seconds`                                                                                               | Période de grâce (en secondes) utilisée pour vérifier si des identifiants avec date d’expiration ont expiré.                                                                                                                                                                                                                                                                                          | `120`                                              |
| `proxy`                                                                                                                   | Configuration du proxy pour l’endpoint S3. Chaque élément `uri` du bloc `proxy` doit contenir une URL de proxy.                                                                                                                                                                                                                                                                                       | -                                                  |
| `connect_timeout_ms`                                                                                                      | Délai d’expiration de connexion du socket, en millisecondes.                                                                                                                                                                                                                                                                                                                                          | `10000` (10 secondes)                              |
| `request_timeout_ms`                                                                                                      | Délai d’expiration de la requête, en millisecondes.                                                                                                                                                                                                                                                                                                                                                   | `5000` (5 secondes)                                |
| `retry_attempts`                                                                                                          | Nombre de tentatives de nouvelle tentative pour les requêtes en échec.                                                                                                                                                                                                                                                                                                                                | `10`                                               |
| `single_read_retries`                                                                                                     | Nombre de tentatives de nouvelle tentative en cas de perte de connexion pendant la lecture.                                                                                                                                                                                                                                                                                                           | `4`                                                |
| `min_bytes_for_seek`                                                                                                      | Nombre minimal d’octets à partir duquel utiliser l’opération seek au lieu d’une lecture séquentielle.                                                                                                                                                                                                                                                                                                 | `1 MB`                                             |
| `metadata_path`                                                                                                           | Chemin du système de fichiers local où stocker les fichiers de métadonnées S3.                                                                                                                                                                                                                                                                                                                        | `/var/lib/clickhouse/disks/<disk_name>/`           |
| `skip_access_check`                                                                                                       | Si `true`, ignore les vérifications d’accès au disque au démarrage.                                                                                                                                                                                                                                                                                                                                   | `false`                                            |
| `header`                                                                                                                  | Ajoute l’en-tête HTTP spécifié aux requêtes. Peut être indiqué plusieurs fois.                                                                                                                                                                                                                                                                                                                        | *                                                  |
| `server_side_encryption_customer_key_base64`                                                                              | En-têtes requis pour accéder aux objets S3 chiffrés avec SSE-C.                                                                                                                                                                                                                                                                                                                                       | -                                                  |
| `server_side_encryption_kms_key_id`                                                                                       | En-têtes requis pour accéder aux objets S3 avec le [chiffrement SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html). Une chaîne vide indique d’utiliser la clé S3 gérée par AWS.                                                                                                                                                                                  | *                                                  |
| `server_side_encryption_kms_encryption_context`                                                                           | En-tête du contexte de chiffrement pour SSE-KMS (utilisé avec `server_side_encryption_kms_key_id`).                                                                                                                                                                                                                                                                                                   | -                                                  |
| `server_side_encryption_kms_bucket_key_enabled`                                                                           | Active les clés de bucket S3 pour SSE-KMS (utilisé avec `server_side_encryption_kms_key_id`).                                                                                                                                                                                                                                                                                                         | Correspond au paramètre défini au niveau du bucket |
| `s3_max_put_rps`                                                                                                          | Nombre maximal de requêtes PUT par seconde avant throttling.                                                                                                                                                                                                                                                                                                                                          | `0` (illimité)                                     |
| `s3_max_put_burst`                                                                                                        | Nombre maximal de requêtes PUT simultanées avant d&#39;atteindre la limite de RPS.                                                                                                                                                                                                                                                                                                                    | Identique à `s3_max_put_rps`                       |
| `s3_max_get_rps`                                                                                                          | Nombre maximal de requêtes GET par seconde avant throttling.                                                                                                                                                                                                                                                                                                                                          | `0` (illimité)                                     |
| `s3_max_get_burst`                                                                                                        | Nombre maximal de requêtes GET simultanées avant d&#39;atteindre la limite de RPS.                                                                                                                                                                                                                                                                                                                    | Identique à `s3_max_get_rps`                       |
| `read_resource`                                                                                                           | Nom de la ressource pour l’[ordonnancement](/fr/operations/workload-scheduling.md) des requêtes de lecture.                                                                                                                                                                                                                                                                                              | Chaîne vide (désactivée)                           |
| `write_resource`                                                                                                          | Nom de la ressource pour l’[ordonnancement](/fr/operations/workload-scheduling.md) des requêtes d’écriture.                                                                                                                                                                                                                                                                                              | Chaîne vide (désactivée)                           |
| `key_template`                                                                                                            | Définit le format de génération des clés d’objet à l’aide de la syntaxe [re2](https://github.com/google/re2/wiki/Syntax). Nécessite le flag `storage_metadata_write_full_object_key`. Incompatible avec `root path` dans `endpoint`. Nécessite `key_compatibility_prefix`.                                                                                                                            | *                                                  |
| `key_compatibility_prefix`                                                                                                | Obligatoire avec `key_template`. Spécifie l’ancien `root path` dans `endpoint` afin de lire d’anciennes versions des métadonnées.                                                                                                                                                                                                                                                                     | -                                                  |
| `read_only`                                                                                                               | Autorise uniquement la lecture à partir du disque.                                                                                                                                                                                                                                                                                                                                                    | *                                                  |
| :::note                                                                                                                   |                                                                                                                                                                                                                                                                                                                                                                                                       |                                                    |
| Google Cloud Storage (GCS) est également pris en charge via le type `s3`. Voir [GCS backed MergeTree](/fr/integrations/gcs). |                                                                                                                                                                                                                                                                                                                                                                                                       |                                                    |
| :::                                                                                                                       |                                                                                                                                                                                                                                                                                                                                                                                                       |                                                    |

<div id="plain-storage">
  ### Utilisation du stockage simple
</div>

Dans `22.10`, un nouveau type de disque `s3_plain` a été introduit ; il fournit un stockage à écriture unique.
Ses paramètres de configuration sont les mêmes que pour le type de disque `s3`.
Contrairement au type de disque `s3`, il stocke les données telles quelles. En d&#39;autres termes,
au lieu d&#39;utiliser des noms de blob générés aléatoirement, il utilise des noms de fichiers ordinaires
(de la même manière que ClickHouse stocke les fichiers sur un disque local) et ne stocke aucune
métadonnée localement. Par exemple, celles-ci sont déduites des données sur `s3`.

Ce type de disque permet de conserver une version statique de la table, car il n&#39;autorise pas
l&#39;exécution de fusions sur les données existantes ni l&#39;insertion de nouvelles
données. Un cas d&#39;utilisation de ce type de disque consiste à y créer des sauvegardes, ce qui peut être fait
via `BACKUP TABLE data TO Disk('plain_disk_name', 'backup_name')`. Ensuite,
vous pouvez faire `RESTORE TABLE data AS data_restored FROM Disk('plain_disk_name', 'backup_name')`
ou utiliser `ATTACH TABLE data (...) ENGINE = MergeTree() SETTINGS disk = 'plain_disk_name'`.

Configuration :

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

À partir de `24.1`, il est possible de configurer n’importe quel disque de stockage d’objets (`s3`, `azure`, `hdfs` (non pris en charge), `local`) en utilisant
le type de métadonnées `plain`.

Configuration :

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>azure</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

<div id="s3-plain-rewritable-storage">
  ### Utilisation du stockage S3 Plain Rewritable
</div>

Un nouveau type de disque `s3_plain_rewritable` a été introduit dans la version `24.4`.
Comme le type de disque `s3_plain`, il ne nécessite pas d’espace de stockage supplémentaire pour les
fichiers de métadonnées. À la place, les métadonnées sont stockées dans S3.
Contrairement au type de disque `s3_plain`, `s3_plain_rewritable` permet d’exécuter des merges
et prend en charge les opérations `INSERT`.
Les [mutations](/fr/sql-reference/statements/alter#mutations) et la réplication des tables ne sont pas prises en charge.

Ce type de disque convient notamment aux tables `MergeTree` non répliquées. Bien que
le type de disque `s3` convienne aux tables `MergeTree` non répliquées, vous pouvez opter
pour le type de disque `s3_plain_rewritable` si vous n’avez pas besoin de métadonnées locales
pour la table et si vous acceptez un ensemble d’opérations limité. Cela peut
être utile, par exemple, pour les tables système.

Configuration :

```xml
<s3_plain_rewritable>
    <type>s3_plain_rewritable</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

est égal à

```xml
<s3_plain_rewritable>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain_rewritable</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

À partir de `24.5`, il est possible de configurer n’importe quel disque de stockage d’objets
(`s3`, `azure`, `local`) à l’aide du type de métadonnées `plain_rewritable`.

<div id="azure-blob-storage">
  ### Utilisation d’Azure Blob Storage
</div>

Les moteurs de table de la famille `MergeTree` peuvent stocker des données dans [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/)
à l’aide d’un disque de type `azure_blob_storage`.

Configuration :

```xml
<storage_configuration>
    ...
    <disks>
        <blob_storage_disk>
            <type>azure_blob_storage</type>
            <storage_account_url>http://account.blob.core.windows.net</storage_account_url>
            <container_name>container</container_name>
            <account_name>account</account_name>
            <account_key>pass123</account_key>
            <metadata_path>/var/lib/clickhouse/disks/blob_storage_disk/</metadata_path>
            <cache_path>/var/lib/clickhouse/disks/blob_storage_disk/cache/</cache_path>
            <skip_access_check>false</skip_access_check>
        </blob_storage_disk>
    </disks>
    ...
</storage_configuration>
```

<div id="azure-blob-storage-connection-parameters">
  #### Paramètres de connexion
</div>

| Paramètre                           | Description                                                                                                                                                                                                                                      | Valeur par défaut   |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------- |
| `storage_account_url` (obligatoire) | URL du compte Azure Blob Storage. Exemples : `http://account.blob.core.windows.net` ou `http://azurite1:10000/devstoreaccount1`.                                                                                                                 | -                   |
| `container_name`                    | Nom du conteneur cible.                                                                                                                                                                                                                          | `default-container` |
| `container_already_exists`          | Contrôle le comportement de création du conteneur : <br />- `false` : crée un nouveau conteneur <br />- `true` : se connecte directement à un conteneur existant <br />- Non défini : vérifie si le conteneur existe, puis le crée si nécessaire | -                   |

Paramètres d’authentification (le disque essaiera toutes les méthodes disponibles **ainsi que** Managed Identity Credential) :

| Paramètre           | Description                                                                   |
| ------------------- | ----------------------------------------------------------------------------- |
| `connection_string` | Pour l’authentification à l’aide d’une connection string.                     |
| `account_name`      | Pour l’authentification à l’aide de Shared Key (utilisé avec `account_key`).  |
| `account_key`       | Pour l’authentification à l’aide de Shared Key (utilisé avec `account_name`). |

<div id="azure-blob-storage-limit-parameters">
  #### Paramètres de limitation
</div>

| Paramètre                            | Description                                                                          |
| ------------------------------------ | ------------------------------------------------------------------------------------ |
| `s3_max_single_part_upload_size`     | Taille maximale d’un téléversement en un seul bloc vers Blob Storage.                |
| `min_bytes_for_seek`                 | Taille minimale d’une région dans laquelle un seek peut être effectué.               |
| `max_single_read_retries`            | Nombre maximal de tentatives pour lire un fragment de données depuis Blob Storage.   |
| `max_single_download_retries`        | Nombre maximal de tentatives pour télécharger un tampon lisible depuis Blob Storage. |
| `thread_pool_size`                   | Nombre maximal de threads pour l’instanciation de `IDiskRemote`.                     |
| `s3_max_inflight_parts_for_one_file` | Nombre maximal de requêtes PUT simultanées pour un seul objet.                       |

<div id="azure-blob-storage-other-parameters">
  #### Autres paramètres
</div>

| Paramètre                        | Description                                                                                              | Valeur par défaut                        |
| -------------------------------- | -------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `metadata_path`                  | Chemin du système de fichiers local où stocker les fichiers de métadonnées pour Blob Storage.            | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`              | Si `true`, ignore les vérifications d’accès au disque au démarrage.                                      | `false`                                  |
| `read_resource`                  | Nom de la ressource pour la [planification](/fr/operations/workload-scheduling.md) des requêtes de lecture. | Chaîne vide (désactivé)                  |
| `write_resource`                 | Nom de la ressource pour la [planification](/fr/operations/workload-scheduling.md) des requêtes d’écriture. | Chaîne vide (désactivé)                  |
| `metadata_keep_free_space_bytes` | Espace libre à réserver sur le disque des métadonnées.                                                   | -                                        |

Vous trouverez des exemples de configurations fonctionnelles dans le répertoire des tests d’intégration (voir par ex. [test&#95;merge&#95;tree&#95;azure&#95;blob&#95;storage](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_merge_tree_azure_blob_storage/configs/config.d/storage_conf.xml) ou [test&#95;azure&#95;blob&#95;storage&#95;zero&#95;copy&#95;replication](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_azure_blob_storage_zero_copy_replication/configs/config.d/storage_conf.xml)).

:::note La réplication zero-copy n’est pas prête pour un usage en production
La réplication zero-copy est désactivée par défaut dans ClickHouse version 22.8 et ultérieure. Cette fonctionnalité n’est pas recommandée pour une utilisation en production.
:::

<div id="using-hdfs-storage-unsupported">
  ## Utilisation du stockage HDFS (Non pris en charge)
</div>

Dans cet exemple de configuration :

* le disque est de type `hdfs` (non pris en charge)
* les données sont stockées à l’emplacement `hdfs://hdfs1:9000/clickhouse/`

À noter que HDFS n’est pas pris en charge ; son utilisation peut donc poser problème. Si vous rencontrez un problème, n’hésitez pas à soumettre une pull request avec le correctif.

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <hdfs>
                <type>hdfs</type>
                <endpoint>hdfs://hdfs1:9000/clickhouse/</endpoint>
                <skip_access_check>true</skip_access_check>
            </hdfs>
            <hdd>
                <type>local</type>
                <path>/</path>
            </hdd>
        </disks>
        <policies>
            <hdfs>
                <volumes>
                    <main>
                        <disk>hdfs</disk>
                    </main>
                    <external>
                        <disk>hdd</disk>
                    </external>
                </volumes>
            </hdfs>
        </policies>
    </storage_configuration>
</clickhouse>
```

Gardez à l’esprit que HDFS peut ne pas fonctionner dans certains cas limites.

<div id="encrypted-virtual-file-system">
  ### Utilisation du chiffrement des données
</div>

Vous pouvez chiffrer les données stockées sur des disques externes [S3](/fr/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3) ou [HDFS](#using-hdfs-storage-unsupported) (non pris en charge), ou sur un disque local. Pour activer le mode de chiffrement, vous devez définir dans le fichier de configuration un disque de type `encrypted` et choisir le disque sur lequel les données seront stockées. Un disque `encrypted` chiffre à la volée tous les fichiers écrits et, lorsque vous lisez des fichiers depuis un disque `encrypted`, il les déchiffre automatiquement. Vous pouvez donc utiliser un disque `encrypted` comme un disque ordinaire.

Exemple de configuration de disque :

```xml
<disks>
  <disk1>
    <type>local</type>
    <path>/path1/</path>
  </disk1>
  <disk2>
    <type>encrypted</type>
    <disk>disk1</disk>
    <path>path2/</path>
    <key>_16_ascii_chars_</key>
  </disk2>
</disks>
```

Par exemple, lorsque ClickHouse écrit les données d&#39;une table dans un fichier `store/all_1_1_0/data.bin` sur `disk1`, ce fichier est en réalité écrit sur le disque physique au chemin `/path1/store/all_1_1_0/data.bin`.

Si le même fichier est écrit sur `disk2`, il l&#39;est en réalité sur le disque physique au chemin `/path1/path2/store/all_1_1_0/data.bin`, en mode chiffré.

<div id="required-parameters-encrypted-disk">
  ### Paramètres requis
</div>

| Paramètre | Type   | Description                                                                                                                                                   |
| --------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`    | String | Doit être défini sur `encrypted` pour créer un disque chiffré.                                                                                                |
| `disk`    | String | Type de disque à utiliser pour le stockage sous-jacent.                                                                                                       |
| `key`     | Uint64 | Clé de chiffrement et de déchiffrement. Peut être spécifiée en hexadécimal à l’aide de `key_hex`. Plusieurs clés peuvent être spécifiées via l’attribut `id`. |

<div id="optional-parameters-encrypted-disk">
  ### Paramètres facultatifs
</div>

| Paramètre        | Type   | Par défaut        | Description                                                                                                                                                     |
| ---------------- | ------ | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`           | String | Répertoire racine | Emplacement sur le disque où les données seront enregistrées.                                                                                                   |
| `current_key_id` | String | -                 | Identifiant de clé utilisé pour le chiffrement. Toutes les clés spécifiées peuvent être utilisées pour le déchiffrement.                                        |
| `algorithm`      | Enum   | `AES_128_CTR`     | Algorithme de chiffrement. Options : <br />- `AES_128_CTR` (clé de 16 octets) <br />- `AES_192_CTR` (clé de 24 octets) <br />- `AES_256_CTR` (clé de 32 octets) |

Exemple de configuration de disque :

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <disk_s3>
                <type>s3</type>
                <endpoint>...
            </disk_s3>
            <disk_s3_encrypted>
                <type>encrypted</type>
                <disk>disk_s3</disk>
                <algorithm>AES_128_CTR</algorithm>
                <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
                <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
        </disks>
    </storage_configuration>
</clickhouse>
```

<div id="using-local-cache">
  ### Utilisation du cache local
</div>

Il est possible de configurer un cache local sur les disques dans la configuration du stockage à partir de la version 22.3.
Pour les versions 22.3 à 22.7, le cache est pris en charge uniquement pour le type de disque `s3`. Pour les versions &gt;= 22.8, le cache est pris en charge pour tous les types de disques : S3, Azure, Local, Encrypted, etc.
Pour les versions &gt;= 23.5, le cache est pris en charge uniquement pour les types de disques distants : S3, Azure, HDFS (non pris en charge).
Le cache utilise la politique de cache `LRU`.

Exemple de configuration pour les versions supérieures ou égales à 22.8 :

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
            </s3>
            <cache>
                <type>cache</type>
                <disk>s3</disk>
                <path>/s3_cache/</path>
                <max_size>10Gi</max_size>
            </cache>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>cache</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

Exemple de configuration pour les versions antérieures à 22.8 :

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
                <data_cache_enabled>1</data_cache_enabled>
                <data_cache_max_size>10737418240</data_cache_max_size>
            </s3>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

Paramètres de **configuration du disque** de File Cache :

Ces paramètres doivent être définis dans la section de configuration du disque.

| Paramètre                             | Type    | Par défaut | Description                                                                                                                                                                                                                                       |
| ------------------------------------- | ------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`                                | Chaîne  | -          | **Obligatoire**. Chemin du répertoire où le cache sera stocké.                                                                                                                                                                                    |
| `max_size`                            | Taille  | -          | **Obligatoire**. Taille maximale du cache en octets ou dans un format lisible (par ex. `10Gi`). Les fichiers sont évincés selon la politique LRU lorsque la limite est atteinte. Prend en charge les formats `ki`, `Mi`, `Gi` (depuis la v22.10). |
| `cache_on_write_operations`           | Booléen | `false`    | Active le cache en écriture directe pour les requêtes `INSERT` et les fusions en arrière-plan. Peut être redéfini au niveau de la requête avec `enable_filesystem_cache_on_write_operations`.                                                     |
| `enable_filesystem_query_cache_limit` | Booléen | `false`    | Active des limites de taille du cache par requête basées sur `max_query_cache_size`.                                                                                                                                                              |
| `enable_cache_hits_threshold`         | Booléen | `false`    | Lorsqu&#39;il est activé, les données ne sont mises en cache qu&#39;après avoir été lues plusieurs fois.                                                                                                                                          |
| `cache_hits_threshold`                | Entier  | `0`        | Nombre de lectures requis avant la mise en cache des données (nécessite `enable_cache_hits_threshold`).                                                                                                                                           |
| `enable_bypass_cache_with_threshold`  | Booléen | `false`    | Ignore le cache pour les grandes plages de lecture.                                                                                                                                                                                               |
| `bypass_cache_threshold`              | Taille  | `256Mi`    | Taille de plage de lecture qui déclenche le contournement du cache (nécessite `enable_bypass_cache_with_threshold`).                                                                                                                              |
| `max_file_segment_size`               | Taille  | `8Mi`      | Taille maximale d&#39;un fichier de cache unique en octets ou dans un format lisible.                                                                                                                                                             |
| `max_elements`                        | Entier  | `10000000` | Nombre maximal de fichiers de cache.                                                                                                                                                                                                              |
| `load_metadata_threads`               | Entier  | `16`       | Nombre de threads pour le chargement des métadonnées du cache au démarrage.                                                                                                                                                                       |
| `use_split_cache`                     | Booléen | `false`    | Sépare les fichiers système des fichiers de données.                                                                                                                                                                                              |
| `split_cache_ratio`                   | Double  | `0.1`      | Proportion du segment système dans la taille totale du cache pour split&#95;cache.                                                                                                                                                                |

> **Remarque** : les valeurs de taille prennent en charge des unités comme `ki`, `Mi`, `Gi`, etc. (par ex. `10Gi`).

<div id="file-cache-query-profile-settings">
  ## Paramètres de requête/profil de File Cache
</div>

| Paramètre                                                               | Type    | Par défaut              | Description                                                                                                                                                                                                                            |
| ----------------------------------------------------------------------- | ------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enable_filesystem_cache`                                               | Boolean | `true`                  | Active ou désactive l&#39;utilisation du cache pour chaque requête, même avec un type de disque `cache`.                                                                                                                               |
| `read_from_filesystem_cache_if_exists_otherwise_bypass_cache`           | Boolean | `false`                 | Lorsqu&#39;il est activé, utilise le cache uniquement si les données s&#39;y trouvent ; les nouvelles données ne seront pas mises en cache.                                                                                            |
| `enable_filesystem_cache_on_write_operations`                           | Boolean | `false` (Cloud: `true`) | Active le cache en écriture directe. Nécessite `cache_on_write_operations` dans la configuration du cache.                                                                                                                             |
| `enable_filesystem_cache_log`                                           | Boolean | `false`                 | Active la journalisation détaillée de l&#39;utilisation du cache dans `system.filesystem_cache_log`.                                                                                                                                   |
| `filesystem_cache_allow_background_download`                            | Boolean | `true`                  | Permet de terminer en arrière-plan les segments partiellement téléchargés. Désactivez cette option pour garder les téléchargements au premier plan pour la requête/session en cours.                                                   |
| `max_query_cache_size`                                                  | Taille  | `false`                 | Taille maximale du cache par requête. Nécessite `enable_filesystem_query_cache_limit` dans la configuration du cache.                                                                                                                  |
| `filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit` | Boolean | `true`                  | Contrôle le comportement lorsque `max_query_cache_size` est atteint : <br />- `true` : arrête le téléchargement de nouvelles données <br />- `false` : évince les anciennes données pour libérer de l&#39;espace aux nouvelles données |

:::warning
Les paramètres de configuration du cache et les paramètres de requête du cache correspondent à la dernière version de ClickHouse ;
dans les versions antérieures, certains éléments peuvent ne pas être pris en charge.
:::

<div id="cache-system-tables-file-cache">
  #### Tables système du cache de système de fichiers
</div>

| Nom de la table               | Description                                                             | Prérequis                                      |
| ----------------------------- | ----------------------------------------------------------------------- | ---------------------------------------------- |
| `system.filesystem_cache`     | Affiche l’état actuel du cache du système de fichiers.                  | Aucun                                          |
| `system.filesystem_cache_log` | Fournit des statistiques détaillées d’utilisation du cache par requête. | Nécessite `enable_filesystem_cache_log = true` |

<div id="cache-commands-file-cache">
  #### Commandes de cache
</div>

<div id="system-clear-filesystem-cache-on-cluster">
  ##### `SYSTEM CLEAR|DROP FILESYSTEM CACHE (<cache_name>) (ON CLUSTER)` -- `ON CLUSTER`
</div>

Cette commande n’est prise en charge que si aucun `<cache_name>` n’est spécifié

<div id="show-filesystem-caches">
  ##### `SHOW FILESYSTEM CACHES`
</div>

Affiche la liste des caches du système de fichiers configurés sur le serveur.
(Pour les versions inférieures ou égales à `22.8`, la commande s&#39;appelle `SHOW CACHES`)

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="describe-filesystem-cache">
  ##### `DESCRIBE FILESYSTEM CACHE '<cache_name>'`
</div>

Affiche la configuration du cache ainsi que quelques statistiques générales pour un cache donné.
Le nom du cache peut être obtenu avec la commande `SHOW FILESYSTEM CACHES`. (Pour les versions inférieures
ou égales à `22.8`, la commande s&#39;appelle `DESCRIBE CACHE`)

```sql title="Query"
DESCRIBE FILESYSTEM CACHE 's3_cache'
```

```text title="Response"
┌────max_size─┬─max_elements─┬─max_file_segment_size─┬─boundary_alignment─┬─cache_on_write_operations─┬─cache_hits_threshold─┬─current_size─┬─current_elements─┬─path───────┬─background_download_threads─┬─enable_bypass_cache_with_threshold─┐
│ 10000000000 │      1048576 │             104857600 │            4194304 │                         1 │                    0 │         3276 │               54 │ /s3_cache/ │                           2 │                                  0 │
└─────────────┴──────────────┴───────────────────────┴────────────────────┴───────────────────────────┴──────────────────────┴──────────────┴──────────────────┴────────────┴─────────────────────────────┴────────────────────────────────────┘
```

| Métriques courantes du cache | Métriques asynchrones du cache | Événements de profil du cache                                                             |
| ---------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------- |
| `FilesystemCacheSize`        | `FilesystemCacheBytes`         | `CachedReadBufferReadFromSourceBytes`, `CachedReadBufferReadFromCacheBytes`               |
| `FilesystemCacheElements`    | `FilesystemCacheFiles`         | `CachedReadBufferReadFromSourceMicroseconds`, `CachedReadBufferReadFromCacheMicroseconds` |
|                              |                                | `CachedReadBufferCacheWriteBytes`, `CachedReadBufferCacheWriteMicroseconds`               |
|                              |                                | `CachedWriteBufferCacheWriteBytes`, `CachedWriteBufferCacheWriteMicroseconds`             |

<div id="web-storage">
  ### Utilisation d&#39;un stockage Web statique (lecture seule)
</div>

Il s&#39;agit d&#39;un disque en lecture seule. Ses données sont uniquement lues et ne sont jamais modifiées. Une nouvelle table
est chargée sur ce disque via une requête `ATTACH TABLE` (voir l&#39;exemple ci-dessous). Le disque local
n&#39;est en fait pas utilisé : chaque requête `SELECT` déclenche une requête `http` pour
récupérer les données nécessaires. Toute modification des données de la table entraînera une
exception, c.-à-d. que les types de requêtes suivants ne sont pas autorisés : [`CREATE TABLE`](/fr/sql-reference/statements/create/table.md),
[`ALTER TABLE`](/fr/sql-reference/statements/alter/index.md), [`RENAME TABLE`](/fr/sql-reference/statements/rename#rename-table),
[`DETACH TABLE`](/fr/sql-reference/statements/detach.md) et [`TRUNCATE TABLE`](/fr/sql-reference/statements/truncate.md).
Le stockage Web peut être utilisé à des fins de lecture seule. Il peut, par exemple, servir à héberger
des données d&#39;exemple ou à migrer des données. Il existe un outil, `clickhouse-static-files-uploader`,
qui prépare un répertoire de données pour une table donnée (`SELECT data_paths FROM system.tables WHERE name = 'table_name'`).
Pour chaque table nécessaire, vous obtenez un répertoire de fichiers. Ces fichiers peuvent être téléversés
vers, par exemple, un serveur web servant des fichiers statiques. Après cette préparation,
vous pouvez charger cette table dans n&#39;importe quel serveur ClickHouse via `DiskWeb`.

Dans cet exemple de configuration :

* le disque est de type `web`
* les données sont hébergées à l&#39;adresse `http://nginx:80/test1/`
* un cache sur le stockage local est utilisé

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>http://nginx:80/test1/</endpoint>
            </web>
            <cached_web>
                <type>cache</type>
                <disk>web</disk>
                <path>cached_web_cache/</path>
                <max_size>100000000</max_size>
            </cached_web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
            <cached_web>
                <volumes>
                    <main>
                        <disk>cached_web</disk>
                    </main>
                </volumes>
            </cached_web>
        </policies>
    </storage_configuration>
</clickhouse>
```

:::tip
Le stockage peut également être configuré temporairement dans une requête, si un jeu de données web
n’est pas destiné à être utilisé régulièrement, consultez la [configuration dynamique](#dynamic-configuration) et évitez
de modifier le fichier de configuration.

Un [jeu de données de démonstration](https://github.com/ClickHouse/web-tables-demo) est hébergé sur GitHub. Pour préparer vos propres tables pour le
stockage web, consultez l’outil [clickhouse-static-files-uploader](/fr/operations/utilities/static-files-disk-uploader)
:::

Dans cette requête `ATTACH TABLE`, le `UUID` fourni correspond au nom du répertoire des données, et l’endpoint est l’URL du contenu brut GitHub.

```sql
-- highlight-next-line
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      );
  -- highlight-end
```

Un cas de test prêt à l’emploi. Vous devez ajouter cette configuration dans la config :

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>https://clickhouse-datasets.s3.yandex.net/disk-with-static-files-tests/test-hits/</endpoint>
            </web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
        </policies>
    </storage_configuration>
</clickhouse>
```

Exécutez ensuite cette requête :

```sql
ATTACH TABLE test_hits UUID '1ae36516-d62d-4218-9ae3-6516d62da218'
(
    WatchID UInt64,
    JavaEnable UInt8,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RegionID UInt32,
    UserID UInt64,
    CounterClass Int8,
    OS UInt8,
    UserAgent UInt8,
    URL String,
    Referer String,
    URLDomain String,
    RefererDomain String,
    Refresh UInt8,
    IsRobot UInt8,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    FlashMinor2 String,
    NetMajor UInt8,
    NetMinor UInt8,
    UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    MobilePhone UInt8,
    MobilePhoneModel String,
    Params String,
    IPNetworkID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    IsArtifical UInt8,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,
    SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,
    PageCharset String,
    CodeVersion UInt32,
    IsLink UInt8,
    IsDownload UInt8,
    IsNotBounce UInt8,
    FUniqID UInt64,
    HID UInt32,
    IsOldCounter UInt8,
    IsEvent UInt8,
    IsParameter UInt8,
    DontCountHits UInt8,
    WithHash UInt8,
    HitColor FixedString(1),
    UTCEventTime DateTime,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),
    SocialNetwork String,
    SocialAction String,
    HTTPError UInt16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    RedirectTiming Int32,
    DOMInteractiveTiming Int32,
    DOMContentLoadedTiming Int32,
    DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,
    LoadEventEndTiming Int32,
    NSToDOMContentLoadedTiming Int32,
    FirstPaintTiming Int32,
    RedirectCount Int8,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    GoalsReached Array(UInt32),
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    RefererHash UInt64,
    URLHash UInt64,
    CLID UInt32,
    YCLID UInt64,
    ShareService String,
    ShareURL String,
    ShareTitle String,
    ParsedParams Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    IslandID FixedString(16),
    RequestNum UInt32,
    RequestTry UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy='web';
```

<div id="required-parameters-s3">
  #### Paramètres requis
</div>

| Paramètre  | Description                                                                                                     |
| ---------- | --------------------------------------------------------------------------------------------------------------- |
| `type`     | `web`. Sinon, le disque n’est pas créé.                                                                         |
| `endpoint` | L’URL de l’endpoint au format `path`. Elle doit contenir un chemin racine pour stocker les données téléversées. |

<div id="optional-parameters-s3">
  #### Paramètres facultatifs
</div>

| Paramètre                           | Description                                                                                                  | Valeur par défaut |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------------- |
| `min_bytes_for_seek`                | Le nombre minimal d’octets à partir duquel utiliser une opération de seek au lieu d’une lecture séquentielle | `1` MB            |
| `remote_fs_read_backoff_threashold` | Le temps d’attente maximal lors d’une tentative de lecture de données sur le disque distant                  | `10000` secondes  |
| `remote_fs_read_backoff_max_tries`  | Le nombre maximal de tentatives de lecture avec backoff                                                      | `5`               |

Si une requête échoue avec l’exception `DB:Exception Unreachable URL`, vous pouvez essayer d’ajuster les paramètres suivants : [http&#95;connection&#95;timeout](/fr/operations/settings/settings.md/#http_connection_timeout), [http&#95;receive&#95;timeout](/fr/operations/settings/settings.md/#http_receive_timeout), [keep&#95;alive&#95;timeout](/fr/operations/server-configuration-parameters/settings#keep_alive_timeout).

Pour obtenir les fichiers à téléverser, exécutez :
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>` (`--metadata-path` peut être obtenu avec la requête `SELECT data_paths FROM system.tables WHERE name = 'table_name'`).

Lors du chargement des fichiers via `endpoint`, ceux-ci doivent être chargés dans le chemin `<endpoint>/store/`, mais la configuration ne doit contenir que `endpoint`.

Si l’URL n’est pas accessible au chargement du disque lorsque le serveur démarre les tables, toutes les erreurs sont interceptées. Si, dans ce cas, des erreurs se produisent, les tables peuvent être rechargées (et redevenir visibles) via `DETACH TABLE table_name` -&gt; `ATTACH TABLE table_name`. Si les métadonnées ont été chargées avec succès au démarrage du serveur, les tables sont immédiatement disponibles.

Utilisez le paramètre [http&#95;max&#95;single&#95;read&#95;retries](/fr/operations/storing-data#web-storage) pour limiter le nombre maximal de tentatives pendant une seule lecture HTTP.

<div id="zero-copy">
  ### Réplication zero-copy (non prête pour la production)
</div>

La réplication zero-copy est possible, mais déconseillée, avec les disques `S3` et `HDFS` (non pris en charge). La réplication zero-copy signifie que, si les données sont stockées à distance sur plusieurs machines et doivent être synchronisées, seules les métadonnées sont répliquées (les chemins vers les data parts), et non les données elles-mêmes.

:::note La réplication zero-copy n&#39;est pas prête pour la production
La réplication zero-copy est désactivée par défaut à partir de la version 22.8 de ClickHouse. Cette fonctionnalité n&#39;est pas recommandée pour une utilisation en production.
:::