---
description: 'Fournit une interface de type table en lecture seule pour les tables Apache Iceberg stockées dans
  Amazon S3, Azure, HDFS ou en local.'
sidebar_label: 'iceberg'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'reference'
---

Fournit une interface de type table en lecture seule pour les tables Apache [Iceberg](https://iceberg.apache.org/) stockées dans Amazon S3, Azure, HDFS ou en local.

<div id="syntax">
  ## Syntaxe
</div>

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Arguments
</div>

La description des arguments est la même que celle des arguments des fonctions de table `s3`, `azureBlobStorage`, `HDFS` et `file`, respectivement.
`format` désigne le format des fichiers de données dans la table Iceberg.

Pour `icebergS3`, vous pouvez utiliser le paramètre facultatif `extra_credentials` pour transmettre un `role_arn` afin d’utiliser l’accès basé sur les rôles dans ClickHouse Cloud. Consultez [Secure S3](/fr/cloud/data-sources/secure-s3) pour connaître les étapes de configuration.

<div id="returned-value">
  ### Valeur retournée
</div>

Une table de la structure spécifiée permettant de lire les données de la table Iceberg spécifiée.

<div id="example">
  ### Exemple
</div>

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
ClickHouse prend actuellement en charge la lecture des versions v1 et v2 du format Iceberg via les fonctions de table `icebergS3`, `icebergAzure`, `icebergHDFS` et `icebergLocal`, ainsi que les moteurs de table `IcebergS3`, `icebergAzure`, `IcebergHDFS` et `IcebergLocal`.
:::

<div id="defining-a-named-collection">
  ## Définir une collection nommée
</div>

Voici un exemple de configuration d’une collection nommée pour y stocker l’URL et les informations d’authentification :

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

<div id="iceberg-writes-catalogs">
  ## Utiliser un catalogue de données
</div>

Les tables Iceberg peuvent également être utilisées avec différents catalogues de données, comme [REST Catalog](https://iceberg.apache.org/rest-catalog-spec/), [AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html) et [Unity Catalog](https://www.unitycatalog.io/).

:::important
Lorsque vous utilisez un catalogue, la plupart des utilisateurs souhaiteront utiliser le moteur de base de données `DataLakeCatalog`, qui connecte ClickHouse à votre catalogue pour y découvrir vos tables. Vous pouvez utiliser ce moteur de base de données au lieu de créer manuellement des tables individuelles avec le moteur de table `IcebergS3`.
:::

Pour les utiliser, créez une table avec le moteur `IcebergS3` et fournissez les paramètres nécessaires.

Par exemple, en utilisant REST Catalog avec le stockage MinIO :

```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
```

Ou, avec AWS Glue Data Catalog et S3 :

```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
```

<div id="schema-evolution">
  ## Évolution du schéma
</div>

À l’heure actuelle, CH permet de lire des tables Iceberg dont le schéma a évolué au fil du temps. La lecture des tables est actuellement prise en charge lorsque des colonnes ont été ajoutées ou supprimées, ou lorsque leur ordre a changé. Vous pouvez également faire passer une colonne obligatoire à une colonne autorisant NULL. De plus, les conversions de type autorisées pour les types simples sont prises en charge, à savoir :  

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) où P&#39; &gt; P.

À ce stade, il n’est pas possible de modifier des structures imbriquées ni les types des éléments dans les types Array et Map.

<div id="partition-pruning">
  ## Élagage des partitions
</div>

ClickHouse prend en charge l’élagage des partitions lors des requêtes SELECT sur les tables Iceberg, ce qui permet d’optimiser les performances des requêtes en ignorant les fichiers de données non pertinents. Pour activer l’élagage des partitions, définissez `use_iceberg_partition_pruning = 1`. Pour plus d’informations sur l’élagage des partitions pour Iceberg, consultez https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## Time travel
</div>

ClickHouse prend en charge le time travel pour les tables Iceberg, ce qui vous permet d&#39;interroger des données historiques à l&#39;aide d&#39;un timestamp spécifique ou d&#39;un ID de snapshot.

<div id="deleted-rows">
  ## Traitement des tables contenant des lignes supprimées
</div>

Actuellement, seules les tables Iceberg avec des [suppressions par position](https://iceberg.apache.org/spec/#position-delete-files) sont prises en charge.

Les méthodes de suppression suivantes ne sont **pas prises en charge** :

* [Suppressions par égalité](https://iceberg.apache.org/spec/#equality-delete-files)
* [Vecteurs de suppression](https://iceberg.apache.org/spec/#deletion-vectors) (introduits en v3)

<div id="basic-usage">
  ### Utilisation de base
</div>

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
```

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
```

Remarque : vous ne pouvez pas spécifier à la fois les paramètres `iceberg_timestamp_ms` et `iceberg_snapshot_id` dans une même requête.

<div id="important-considerations">
  ### Considérations importantes
</div>

* **Les snapshots** sont généralement créés lorsque :

* De nouvelles données sont écrites dans la table

* Une opération de compaction des données est effectuée

* **Les modifications de schéma ne créent généralement pas de snapshots** - Cela entraîne des comportements importants lors de l&#39;utilisation du time travel avec des tables ayant subi une évolution du schéma.

<div id="example-scenarios">
  ### Exemples de scénarios
</div>

Tous les scénarios sont rédigés avec Spark, car CH ne prend pas encore en charge l’écriture dans des tables Iceberg.

<div id="scenario-1">
  #### Scénario 1 : changements de schéma sans nouveaux snapshots
</div>

Prenons la séquence d’opérations suivante :

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

Résultat de la requête à différents horodatages :

* À ts1 &amp; ts2 : seules les deux colonnes d’origine sont affichées
* À ts3 : les trois colonnes sont affichées, avec NULL pour le prix de la première ligne

<div id="scenario-2">
  #### Scénario 2 : différences entre le schéma historique et le schéma actuel
</div>

Une requête de time travel exécutée à l’instant présent peut afficher un schéma différent de celui de la table actuelle :

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

Cela s’explique par le fait que `ALTER TABLE` ne crée pas de nouveau snapshot ; pour la table actuelle, Spark récupère la valeur de `schema_id` à partir du dernier fichier de métadonnées, et non d’un snapshot.

<div id="scenario-3">
  #### Scénario 3 : différences entre le schéma historique et le schéma actuel
</div>

Le second point est que, lors d’un time travel, vous ne pouvez pas obtenir l’état de la table avant que des données n’y aient été écrites :

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

Dans ClickHouse, le comportement est le même que dans Spark. Vous pouvez considérer les requêtes SELECT de Spark comme des requêtes SELECT de ClickHouse, et cela fonctionnera de la même façon.

<div id="metadata-file-resolution">
  ## Résolution du fichier de métadonnées
</div>

Lorsque vous utilisez la fonction de table `iceberg` dans ClickHouse, le système doit localiser le bon fichier metadata.json, qui décrit la structure de la table Iceberg. Voici comment fonctionne ce processus de résolution :

<div id="candidate-search">
  ### Recherche des fichiers candidats (par ordre de priorité)
</div>

1. **Spécification directe du chemin** :
   *Si vous définissez `iceberg_metadata_file_path`, le système utilisera exactement ce chemin en le combinant avec le path du répertoire de la table Iceberg.

* Lorsque ce paramètre est fourni, tous les autres paramètres de résolution sont ignorés.

2. **Correspondance avec l’UUID de la table** :
   *Si `iceberg_metadata_table_uuid` est spécifié, le système :
   *Examinera uniquement les fichiers `.metadata.json` du répertoire `metadata`
   *Filtrera les fichiers contenant un field `table-uuid` correspondant à l’UUID spécifié (sans tenir compte de la casse)

3. **Recherche par défaut** :
   *Si aucun des paramètres ci-dessus n’est fourni, tous les fichiers `.metadata.json` du répertoire `metadata` deviennent des candidats

<div id="most-recent-file">
  ### Sélection du fichier le plus récent
</div>

Après avoir identifié les fichiers candidats à l’aide des règles ci-dessus, le système détermine lequel est le plus récent :

* Si `iceberg_recent_metadata_file_by_last_updated_ms_field` est activé :

* Le fichier dont la valeur `last-updated-ms` est la plus élevée est sélectionné

* Sinon :

* Le fichier dont le numéro de version est le plus élevé est sélectionné

* (La version apparaît sous la forme de `V` dans les noms de fichiers au format `V.metadata.json` ou `V-uuid.metadata.json`)

**Remarque** : Tous les paramètres mentionnés sont des paramètres de fonction de table (et non des paramètres globaux ou au niveau de la requête) et doivent être spécifiés comme indiqué ci-dessous :

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**Remarque** : Bien que les catalogues Iceberg gèrent généralement la résolution des métadonnées, la fonction de table `iceberg` dans ClickHouse interprète directement les fichiers stockés dans S3 comme des tables Iceberg, d’où l’importance de comprendre ces règles de résolution.

<div id="metadata-cache">
  ## Cache de métadonnées
</div>

Le moteur de table `Iceberg` et la fonction de table prennent en charge un cache de métadonnées qui stocke les informations des fichiers manifest, de la manifest list et du fichier JSON de métadonnées. Ce cache est stocké en mémoire. Cette fonctionnalité est contrôlée par le paramètre `use_iceberg_metadata_files_cache`, activé par défaut.

<div id="aliases">
  ## Alias
</div>

La fonction de table `iceberg` est désormais un alias de `icebergS3`.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin du fichier. Type : `LowCardinality(String)`.
* `_file` — Nom du fichier. Type : `LowCardinality(String)`.
* `_size` — Taille du fichier en octets. Type : `Nullable(UInt64)`. Si la taille du fichier est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de la dernière modification du fichier. Type : `Nullable(DateTime)`. Si la date et l’heure sont inconnues, la valeur est `NULL`.
* `_etag` — ETag du fichier. Type : `LowCardinality(String)`. Si l’ETag est inconnu, la valeur est `NULL`.

<div id="writes-into-iceberg-table">
  ## Écriture dans une table Iceberg
</div>

À partir de la version 25.7, ClickHouse prend en charge la modification des tables Iceberg des utilisateurs.

Il s’agit actuellement d’une fonctionnalité expérimentale, vous devez donc d’abord l’activer :

```sql
SET allow_insert_into_iceberg = 1;
```

<div id="create-iceberg-table">
  ### Création d’une table Iceberg
</div>

Pour créer votre propre table Iceberg vide, utilisez les mêmes commandes que pour la lecture, mais indiquez explicitement le schéma.
L’écriture prend en charge tous les formats de données de la spécification Iceberg, tels que Parquet, Avro et ORC.

<div id="example">
  ### Exemple
</div>

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

Remarque : pour créer un fichier d’indication de version, activez le paramètre `iceberg_use_version_hint`.
Si vous souhaitez compresser le fichier metadata.json, spécifiez le nom du codec dans le paramètre `iceberg_metadata_compression_method`.

<div id="writes-inserts">
  ### INSERT
</div>

Après avoir créé une nouvelle table, vous pouvez insérer des données en utilisant la syntaxe ClickHouse standard.

<div id="example">
  ### Exemple
</div>

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-delete">
  ### DELETE
</div>

La suppression de lignes excédentaires au format merge-on-read est également prise en charge par ClickHouse.
Cette requête créera un nouveau snapshot avec des fichiers de suppression par position.

<div id="example">
  ### Exemple
</div>

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-schema-evolution">
  ### Évolution du schéma
</div>

ClickHouse vous permet d’ajouter, de supprimer, de modifier ou de renommer des colonnes de types simples (c’est-à-dire ni Tuple, ni Array, ni Map).

<div id="example">
  ### Exemple
</div>

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

<div id="iceberg-writes-compaction">
  ### Compaction
</div>

ClickHouse prend en charge la compaction des tables Iceberg. À ce jour, elle permet de fusionner les fichiers de suppression par position avec les fichiers de données tout en mettant à jour les métadonnées. Les identifiants de snapshot et les horodatages précédents restent inchangés ; la fonctionnalité de time travel peut donc toujours être utilisée avec les mêmes valeurs.

Comment l&#39;utiliser :

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-expire-snapshots">
  ### Expiration des snapshots
</div>

Les tables Iceberg accumulent des snapshots à chaque opération `INSERT`, `DELETE` ou `UPDATE`. Avec le temps, cela peut entraîner un grand nombre de snapshots et de fichiers de données associés. La commande `expire_snapshots` supprime les anciens snapshots et nettoie les fichiers de données qui ne sont plus référencés par aucun snapshot conservé.

**Syntaxe :**

```sql
ALTER TABLE iceberg_table EXECUTE expire_snapshots(
    ['timestamp']
    [, expire_before = 'timestamp']
    [, retention_period = '3d']
    [, retain_last = 100]
    [, snapshot_ids = [1, 2, 3, 4]]
    [, dry_run = 1]
);
```

Par défaut, les snapshots à conserver sont déterminés par la [politique de rétention](#iceberg-snapshot-retention-policy) (propriétés de table `min-snapshots-to-keep`, `max-snapshot-age-ms` et surcharges par référence). Lorsque `snapshot_ids` est spécifié, la politique de rétention est contournée et seuls les snapshots listés sont pris en compte pour l’expiration.

**Arguments :**

* `'timestamp'` (positionnel) ou `expire_before = 'timestamp'` — une chaîne de date et d’heure (par ex. `'2024-06-01 00:00:00'`) interprétée dans le **fuseau horaire du serveur**. Sert de garde-fou : les snapshots dont `timestamp-ms` est supérieur ou égal à cette valeur sont protégés contre l’expiration, même si la politique de rétention les ferait autrement expirer. Peut être combiné avec `snapshot_ids`, auquel cas les snapshots listés correspondant à cet horodatage ou plus récents n’expirent pas.
* `retention_period = '<duration>'` — surcharge `history.expire.max-snapshot-age-ms` défini au niveau de la table, pour cette invocation uniquement. Les snapshots plus anciens que cette durée (mesurée à partir de maintenant) deviennent candidats à l’expiration. La valeur est une chaîne de durée constituée d’une ou plusieurs paires `{number}{unit}` concaténées. Unités prises en charge : `y` (365 jours), `w` (7 jours), `d` (24 heures), `h` (60 minutes), `m` (60 secondes), `s` (1 seconde), `ms` (1 milliseconde). Les unités peuvent être combinées, par ex. `'3d'`, `'12h'`, `'1d12h30m'`, `'500ms'`.
* `retain_last = N` — surcharge `history.expire.min-snapshots-to-keep` défini au niveau de la table, pour cette invocation uniquement. Au moins `N` snapshots sont toujours conservés, quel que soit leur âge.
* `snapshot_ids = [id1, id2, ...]` — fait expirer exactement les ID de snapshot listés (à l’exception des snapshots référencés par le snapshot actuel, les branches ou les tags). Ce mode contourne entièrement la politique de rétention et ne peut pas être combiné avec `retention_period` ni `retain_last`.
* `dry_run = 1` — calcule ce qui expirerait et renvoie des métriques sans écrire de nouvelles métadonnées ni supprimer de fichiers.

:::note
`retention_period` et `retain_last` surchargent uniquement les valeurs de rétention par défaut **au niveau de la table**. Les surcharges de rétention par référence (branch/tag) configurées dans les propriétés de table Iceberg (par ex. `refs.<branch>.min-snapshots-to-keep`) ne sont jamais surchargées — elles s’appliquent toujours comme spécifié dans les métadonnées de la table.
:::

**Exemple :**

```sql
SET allow_insert_into_iceberg = 1;

-- Create some snapshots by inserting data
INSERT INTO iceberg_table VALUES (1);
INSERT INTO iceberg_table VALUES (2);
INSERT INTO iceberg_table VALUES (3);

-- Expire using retention policy only
ALTER TABLE iceberg_table EXECUTE expire_snapshots();

-- Expire with a safety fuse: protect snapshots newer than the timestamp (positional syntax)
ALTER TABLE iceberg_table EXECUTE expire_snapshots('2025-01-01 00:00:00');

-- Same using the named argument form
ALTER TABLE iceberg_table EXECUTE expire_snapshots(expire_before = '2025-01-01 00:00:00');

-- Override retention parameters for one execution
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '3d', retain_last = 10);

-- Expire explicit snapshots
ALTER TABLE iceberg_table EXECUTE expire_snapshots(snapshot_ids = [101, 102, 103]);

-- Dry-run preview (no metadata updates, no file deletes)
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '1d', dry_run = 1);
```

**Sortie :**

La commande renvoie une table comportant deux colonnes (`metric_name String`, `metric_value Int64`) et contenant une ligne par métrique. Les noms des métriques suivent la [spécification Iceberg](https://iceberg.apache.org/docs/latest/spark-procedures/#output) :

| metric&#95;name                       | Description                                                              |
| ------------------------------------- | ------------------------------------------------------------------------ |
| `deleted_data_files_count`            | Nombre de fichiers de données supprimés                                  |
| `deleted_position_delete_files_count` | Nombre de fichiers de suppression par position supprimés                 |
| `deleted_equality_delete_files_count` | Nombre de fichiers de suppression par égalité supprimés                  |
| `deleted_manifest_files_count`        | Nombre de manifest files supprimés                                       |
| `deleted_manifest_lists_count`        | Nombre de fichiers de liste de manifests supprimés                       |
| `deleted_statistics_files_count`      | Nombre de fichiers de statistiques supprimés (toujours à 0 actuellement) |
| `dry_run`                             | `1` pour le mode simulation, `0` pour l’exécution normale                |

La commande effectue les étapes suivantes :

1. Évalue la politique de rétention (voir ci-dessous) afin de déterminer quels snapshots doivent être conservés
2. Si un argument d’horodatage a été fourni, protège également tous les snapshots à cet horodatage ou plus récents
3. Fait expirer les snapshots qui ne sont ni conservés par la politique ni protégés par l’horodatage
4. Détermine quels fichiers sont associés exclusivement à des snapshots expirés
5. En mode normal : génère de nouvelles métadonnées sans les snapshots expirés
6. En mode normal : supprime physiquement les listes de manifests, les manifest files et les fichiers de données inaccessibles
7. En mode `dry_run = 1` : ignore les étapes 5 et 6 et renvoie uniquement les métriques calculées

<div id="iceberg-snapshot-retention-policy">
  #### Politique de rétention des snapshots
</div>

La commande `expire_snapshots` respecte la [politique de rétention des snapshots d’Iceberg](https://iceberg.apache.org/spec/#snapshot-retention-policy). La rétention se configure via les propriétés de table Iceberg et des surcharges par référence :

| Propriété                              | Portée | Par défaut                                                                     | Description                                                                                                      |
| -------------------------------------- | ------ | ------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `history.expire.min-snapshots-to-keep` | Table  | `iceberg_expire_default_min_snapshots_to_keep` (par défaut `1`)                | Nombre minimal de snapshots à conserver dans la chaîne des ancêtres de chaque branche                           |
| `history.expire.max-snapshot-age-ms`   | Table  | `iceberg_expire_default_max_snapshot_age_ms` (par défaut `432000000`, 5 jours) | Âge maximal (en ms) des snapshots à conserver dans une branche                                                 |
| `history.expire.max-ref-age-ms`        | Table  | `iceberg_expire_default_max_ref_age_ms` (par défaut `∞`)                       | Âge maximal (en ms) d’une référence de snapshot (branche ou tag) avant la suppression de la référence elle-même |

Chaque référence de snapshot (`refs` dans les métadonnées Iceberg) peut surcharger ces valeurs à l’aide de champs propres à la référence : `min-snapshots-to-keep`, `max-snapshot-age-ms` et `max-ref-age-ms`.

**Évaluation de la rétention :**

* **Pour chaque branche** (y compris `main`) : la chaîne des ancêtres est parcourue à partir de la tête de branche. Les snapshots sont conservés tant que l’une des conditions suivantes est vraie :
  * Le snapshot fait partie des `min-snapshots-to-keep` premiers de la chaîne
  * L’âge du snapshot reste inférieur ou égal à `max-snapshot-age-ms` (c.-à-d. `now - timestamp-ms <= max-snapshot-age-ms`)
* **Pour les tags** : le snapshot tagué est conservé, sauf si le tag a dépassé son `max-ref-age-ms`, auquel cas la référence du tag est supprimée
* **Les références autres que `main`** dont l’âge dépasse `max-ref-age-ms` sont supprimées intégralement (la branche `main` n’est jamais supprimée)
* **Les références orphelines** qui pointent vers des snapshots inexistants sont supprimées avec un avertissement
* **Le snapshot actuel est toujours conservé**, quels que soient les paramètres de rétention

**Privilèges requis :**

Le privilège `ALTER TABLE EXECUTE` est requis. Il s’agit d’un privilège enfant de `ALTER TABLE` dans la hiérarchie du contrôle d’accès de ClickHouse. Vous pouvez l’accorder spécifiquement ou via le parent :

```sql
-- Grant only EXECUTE permission
GRANT ALTER TABLE EXECUTE ON my_iceberg_table TO my_user;

-- Or grant all ALTER TABLE permissions (includes ALTER TABLE EXECUTE)
GRANT ALTER TABLE ON my_iceberg_table TO my_user;
```

:::note

* Seules les tables au format Iceberg version 2 sont prises en charge (les snapshots v1 ne garantissent pas `manifest-list`, qui est nécessaire pour identifier en toute sécurité les fichiers à supprimer)
* Le snapshot actuel est toujours conservé, même s’il est antérieur au timestamp spécifié
* Nécessite que le paramètre `allow_insert_into_iceberg` soit activé
* Nécessite que le paramètre `allow_experimental_expire_snapshots` soit activé
* L’autorisation propre au catalogue (authentification du catalogue REST, AWS Glue IAM, etc.) s’applique indépendamment lorsque ClickHouse met à jour les métadonnées
  :::

<div id="iceberg-remove-orphan-files">
  ### Supprimer les fichiers orphelins
</div>

Les fichiers orphelins sont des fichiers présents dans le stockage qui ne sont référencés par aucun snapshot dans les métadonnées de la table Iceberg. Ils s’accumulent à la suite d’écritures ayant échoué, d’un nettoyage partiel après la compaction et d’opérations interrompues, ce qui entraîne une croissance incontrôlée du stockage. La commande `remove_orphan_files` identifie et supprime ces fichiers orphelins.

**Syntaxe :**

```sql
-- Positional form: single unnamed older_than argument
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('timestamp')

-- Named form
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = 'timestamp',
    location = 'path',
    dry_run = 0|1
)

-- No arguments: use all defaults (older_than = 3 days ago)
ALTER TABLE iceberg_table EXECUTE remove_orphan_files()
```

**Paramètres :**

| Paramètre    | Type                 | Par défaut                                                                  | Description                                                                                                                                                                                       |
| ------------ | -------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `older_than` | `String` (timestamp) | il y a 3 jours (configurable via `iceberg_orphan_files_older_than_seconds`) | Ne considère comme candidats orphelins que les fichiers dont la date de dernière modification est antérieure à ce timestamp. Garde-fou pour éviter de supprimer des fichiers en cours d’écriture. |
| `location`   | `String`             | Emplacement de la table                                                     | Limite l’analyse à un sous-répertoire précis sous l’emplacement de la table (par exemple, `'data/'` ou `'metadata/'`).                                                                            |
| `dry_run`    | `UInt64`             | `0`                                                                         | Lorsqu’il vaut `1`, identifie les fichiers orphelins et renvoie un résumé des résultats sans rien supprimer réellement.                                                                           |

**Exemples :**

```sql
-- Remove orphan files older than a specific timestamp
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('2026-03-01 00:00:00');

-- Dry run: preview which files would be deleted
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(dry_run = 1);

-- Scan only the data directory
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = '2026-03-01 00:00:00',
    location = 'data/'
);

-- Combine positional older_than with named arguments
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    '2026-03-01 00:00:00',
    dry_run = 1
);
```

**Sortie :**

La commande renvoie un tableau avec les colonnes `metric_name` et `metric_value`, indiquant le nombre de fichiers supprimés (ou qui seraient supprimés en mode dry&#95;run) par catégorie. Les catégories de fichiers sont déterminées à l’aide d’heuristiques basées, dans la mesure du possible, sur les conventions de nommage des fichiers ; les fichiers qui ne correspondent à aucun motif spécifique sont comptabilisés par défaut dans `deleted_data_files_count` :

| metric&#95;name                                     | metric&#95;value |
| --------------------------------------------------- | ---------------- |
| deleted&#95;data&#95;files&#95;count                | 5                |
| deleted&#95;position&#95;delete&#95;files&#95;count | 2                |
| deleted&#95;equality&#95;delete&#95;files&#95;count | 0                |
| deleted&#95;manifest&#95;files&#95;count            | 3                |
| deleted&#95;manifest&#95;lists&#95;count            | 1                |
| deleted&#95;metadata&#95;files&#95;count            | 0                |
| deleted&#95;statistics&#95;files&#95;count          | 0                |
| skipped&#95;missing&#95;metadata&#95;count          | 0                |
| failed&#95;deletions&#95;count                      | 0                |

**Paramètres :**

| Paramètre                                 | Type     | Par défaut         | Description                                                                   |
| ----------------------------------------- | -------- | ------------------ | ----------------------------------------------------------------------------- |
| `allow_iceberg_remove_orphan_files`       | `Bool`   | `false`            | Paramètre de contrôle permettant d’activer la fonctionnalité (expérimentale). |
| `iceberg_orphan_files_older_than_seconds` | `UInt64` | `259200` (3 jours) | Seuil `older_than` par défaut, en secondes, lorsque l’argument est omis.      |

:::note

* **Nécessite Iceberg format version 2 (ou supérieure).** Les tables de version 1 sont rejetées, car elles ne disposent pas de pointeurs `manifest-list` dans les snapshots, indispensables pour déterminer en toute sécurité l’ensemble des fichiers atteignables. L’exécution de la commande sur une table v1 renvoie une erreur `BAD_ARGUMENTS`.
* Les paramètres `allow_insert_into_iceberg` et `allow_iceberg_remove_orphan_files` doivent tous deux être activés
* Il est recommandé d’exécuter `expire_snapshots` avant `remove_orphan_files` afin que les fichiers référencés uniquement par des snapshots expirés soient supprimés en premier
* Utilisez `dry_run = 1` pour prévisualiser les fichiers orphelins avant leur suppression
* Le seuil `older_than` évite de supprimer des fichiers provenant d’écritures en cours — le seuil par défaut de 3 jours offre une marge de sécurité confortable
  :::

<div id="see-also">
  ## Voir aussi
</div>

* [Moteur Iceberg](/fr/engines/table-engines/integrations/iceberg.md)
* [Fonction de table cluster Iceberg](/fr/sql-reference/table-functions/icebergCluster.md)