---
description: 'Ce moteur fournit une intégration en lecture seule avec des tables Apache Paimon existantes sur Amazon S3, Azure, HDFS ou stockées localement.'
sidebar_label: 'Paimon'
sidebar_position: 95
slug: /engines/table-engines/integrations/paimon
title: 'Moteur de table Paimon'
doc_type: 'référence'
---

Ce moteur fournit une intégration en lecture seule avec des tables Apache [Paimon](https://paimon.apache.org/) existantes sur Amazon S3, Azure, HDFS ou stockées localement.
Il prend en charge les lectures d’instantanés, les lectures incrémentielles et l’élagage de partitions de base assuré par le moteur.

<div id="create-table">
  ## Créer une table
</div>

Notez que la table Paimon doit déjà exister dans le stockage ; cette commande n’accepte pas de paramètres DDL pour créer une nouvelle table.
La création de tables `Paimon*` est contrôlée par `allow_experimental_paimon_storage_engine` (désactivé par défaut) ; activez-le donc avant d’exécuter `CREATE TABLE`.

```sql
SET allow_experimental_paimon_storage_engine = 1;

CREATE TABLE paimon_table_s3
    ENGINE = PaimonS3(url,  [, access_key_id, secret_access_key] [,format] [,compression])

CREATE TABLE paimon_table_azure
    ENGINE = PaimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

CREATE TABLE paimon_table_hdfs
    ENGINE = PaimonHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE paimon_table_local
    ENGINE = PaimonLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## Arguments du moteur
</div>

La description des arguments est la même que celle des arguments des moteurs `S3`, `AzureBlobStorage`, `HDFS` et `File`, respectivement.
`format` désigne le format des fichiers de données de la table Paimon.

Les paramètres du moteur peuvent être spécifiés à l’aide de [Collections nommées](../../../operations/named-collections.md)

<div id="example">
  ### Exemple
</div>

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Utilisation des collections nommées :

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3(paimon_conf, filename = 'test_table')
```

<div id="capabilities">
  ## Fonctionnalités
</div>

* Lecture à partir du dernier snapshot de table.
* Lectures incrémentielles basées sur l’identifiant du snapshot validé lorsqu’elles sont activées.
* Élagage des partitions lorsque `use_paimon_partition_pruning` est activé.
* Actualisation facultative des métadonnées en arrière-plan lorsqu’elle est configurée.
* UUID de table stable lors de l’utilisation de bases de données Atomic/Replicated, permettant d’utiliser les macros `{uuid}` dans les chemins Keeper.

<div id="settings">
  ## Paramètres
</div>

Ce moteur utilise les mêmes paramètres que les moteurs de stockage objet correspondants et ajoute des paramètres spécifiques à Paimon :

* `allow_experimental_paimon_storage_engine` — active la création des moteurs de table `Paimon`, `PaimonS3`, `PaimonAzure`, `PaimonHDFS` et `PaimonLocal`. Par défaut : `0` (désactivé).
* `paimon_incremental_read` — active le mode de lecture incrémentielle.
* `paimon_metadata_refresh_interval_sec` — intervalle d’actualisation des métadonnées en arrière-plan, en secondes. Lorsqu’il est défini sur une valeur supérieure à 0, une tâche en arrière-plan récupère périodiquement le dernier snapshot et le schéma depuis le stockage objet. Par défaut : 30.
* `paimon_keeper_path` — chemin Keeper pour l’état de la lecture incrémentielle. Doit être défini et unique pour chaque table ; prend en charge des macros telles que `{database}`, `{table}`, `{uuid}`.
* `paimon_replica_name` — nom de réplique pour l’état de la lecture incrémentielle. Doit être défini et unique pour chaque réplique ; prend en charge des macros telles que `{replica}`.

<div id="incremental-read-examples">
  ## Exemples de lecture incrémentielle
</div>

Lecture incrémentielle avec l’état de Keeper :

```sql
CREATE TABLE paimon_inc
ENGINE = PaimonS3(paimon_conf, filename = 'paimon_all_types')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/{database}/{uuid}',
    paimon_replica_name = '{replica}';
```

<div id="query-level-settings-for-incremental-read">
  ### Paramètres de requête pour la lecture incrémentielle
</div>

Les paramètres suivants sont **au niveau de la requête** (transmis via `SELECT ... SETTINGS`, et non dans `CREATE TABLE`). Ils contrôlent le comportement des lectures incrémentielles pour chaque requête :

* `paimon_target_snapshot_id` — lit uniquement le delta du snapshot spécifié. Le watermark validé dans Keeper n&#39;est **pas** avancé, de sorte que le même snapshot peut être relu un nombre illimité de fois. Par défaut : `-1` (désactivé).
* `max_consume_snapshots` — nombre maximal de snapshots à consommer lors d&#39;une seule lecture incrémentielle. Lorsque la source a accumulé de nombreux snapshots non lus, ce paramètre limite le nombre de snapshots consommés par requête afin de contrôler la taille du Batch. `0` signifie aucune limite. Par défaut : `0`.

**Lecture ciblée d&#39;un snapshot** — renvoie toujours le delta du snapshot 1, quel que soit le watermark actuel :

```sql
SELECT count()
FROM paimon_inc
SETTINGS paimon_target_snapshot_id = 1;
```

**Limiter le nombre de snapshots par lot** — si trois nouveaux snapshots sont en attente, n’en consommer que deux au maximum par requête :

```sql
SELECT count()
FROM paimon_inc
SETTINGS max_consume_snapshots = 2;
```

<div id="paimon-to-mergetree-via-refresh-mv">
  ## De Paimon à MergeTree via une vue matérialisée actualisable
</div>

Vous pouvez créer un pipeline de bout en bout qui synchronise en continu les données d&#39;une table Paimon vers une table MergeTree à l&#39;aide d&#39;une vue matérialisée actualisable en mode `APPEND`. Chaque cycle d&#39;actualisation lit uniquement les nouvelles données incrémentielles de Paimon et les ajoute à la table de destination.

**Étape 1 — Créez la table source Paimon avec la lecture incrémentielle et l&#39;actualisation des métadonnées activées.**

L&#39;exemple ci-dessous utilise `PaimonLocal`. Remplacez le moteur par `PaimonS3`, `PaimonAzure`, `PaimonHDFS` ou le moteur `Paimon` avec détection automatique, selon votre backend de stockage :

```sql
SET allow_experimental_paimon_storage_engine = 1;

-- Local storage
CREATE TABLE paimon_mv_source
ENGINE = PaimonLocal('/path/to/paimon/table')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;

-- S3 storage (the `Paimon` engine defaults to the S3 implementation when no `disk` is specified)
CREATE TABLE paimon_mv_source
ENGINE = Paimon('http://minio:9000/bucket/path/to/table', 'access_key', 'secret_key')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;
```

`paimon_metadata_refresh_interval_sec` définit l’intervalle de rafraîchissement des métadonnées en arrière-plan, en secondes. Lorsqu’il est supérieur à 0, une tâche en arrière-plan récupère périodiquement le dernier instantané et le schéma depuis le stockage objet, afin que le cycle de rafraîchissement de la MV puisse voir les données récemment validées sans attendre qu’une requête déclenche la mise à jour des métadonnées. La valeur par défaut est 30. Utilisez ce paramètre avec précaution sur un grand nombre de tables afin d’éviter des E/S excessives sur le stockage objet et Keeper.

**Étape 2 — Créez la table de destination MergeTree (schéma cloné à partir de la table Paimon) :**

```sql
CREATE TABLE paimon_mv_dest AS paimon_mv_source
ENGINE = MergeTree()
ORDER BY tuple();
```

**Étape 3 — Créez la vue matérialisée actualisable :**

```sql
CREATE MATERIALIZED VIEW paimon_mv
REFRESH EVERY 10 SECOND
APPEND
TO paimon_mv_dest
AS SELECT * FROM paimon_mv_source;
```

Toutes les 10 secondes, la MV exécute `SELECT * FROM paimon_mv_source`, qui ne renvoie que les lignes ajoutées depuis le dernier snapshot validé, puis les ajoute à `paimon_mv_dest`.

**Nettoyage :**

```sql
SYSTEM STOP VIEW paimon_mv;
DROP VIEW IF EXISTS paimon_mv SYNC;
DROP TABLE IF EXISTS paimon_mv_dest SYNC;
DROP TABLE IF EXISTS paimon_mv_source SYNC;
```

:::note
Arrêtez la vue matérialisée avant de la supprimer afin d’empêcher le rafraîchissement en arrière-plan de bloquer les opérations DDL.
:::

<div id="limitations">
  ## Limitations
</div>

* La lecture incrémentielle nécessite que Keeper (ZooKeeper) soit configuré.
* La lecture incrémentielle nécessite que `paimon_keeper_path` soit défini et unique pour chaque table.
* `paimon_replica_name` doit être unique pour chaque réplique dans le même chemin Keeper.
* La lecture incrémentielle utilise une livraison « au plus une fois » : le snapshot validé progresse lorsque les fichiers de données sont collectés, avant que les données ne soient réellement consommées. Si la requête échoue après la collecte des fichiers, les snapshots ignorés ne seront pas relus lors d&#39;une nouvelle tentative.
* Le moteur de table est en lecture seule ; la modification des données n&#39;est pas prise en charge.
* La lecture incrémentielle ne gère pas les suppressions de données historiques depuis la source Paimon. Si les données Paimon en amont sont supprimées ou mises à jour, les lignes correspondantes déjà écrites dans une table de destination MergeTree de ClickHouse ne seront pas automatiquement supprimées. Vous devez exécuter manuellement `ALTER TABLE ... DELETE` sur la table MergeTree afin de nettoyer les données obsolètes.

<div id="aliases">
  ## Alias
</div>

Le moteur de table `Paimon` détecte automatiquement le backend de stockage à partir du paramètre `disk` et redirige vers `PaimonS3`, `PaimonAzure` ou `PaimonLocal` selon le cas. Lorsqu&#39;aucun `disk` n&#39;est spécifié, l&#39;implémentation `PaimonS3` est utilisée par défaut.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette information est inconnue, la valeur est `NULL`.
* `_etag` — eTag du fichier. Type : `LowCardinality(String)`. Si l’eTag est inconnu, la valeur est `NULL`.

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
  ## Partitions prises en charge
</div>

Types de données pris en charge dans les clés de partition Paimon :

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