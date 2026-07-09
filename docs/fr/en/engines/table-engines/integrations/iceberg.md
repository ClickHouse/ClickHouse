---
description: 'Ce moteur fournit une intégration en lecture seule avec des tables Apache Iceberg
  existantes dans Amazon S3, Azure, HDFS et des tables stockées localement.'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'Moteur de table Iceberg'
doc_type: 'reference'
---

:::warning
Nous recommandons d&#39;utiliser la [fonction de table Iceberg](/fr/sql-reference/table-functions/iceberg.md) pour travailler avec des données Iceberg dans ClickHouse. La fonction de table Iceberg offre actuellement des fonctionnalités suffisantes, avec une interface partielle en lecture seule pour les tables Iceberg.

Le moteur de table Iceberg est disponible, mais peut présenter certaines limites. ClickHouse n&#39;a pas été conçu à l&#39;origine pour prendre en charge des tables dont les schémas changent de manière externe, ce qui peut affecter le fonctionnement du moteur de table Iceberg. Par conséquent, certaines fonctionnalités qui fonctionnent avec des tables classiques peuvent être indisponibles ou ne pas fonctionner correctement, en particulier lors de l&#39;utilisation de l&#39;ancien analyseur.

Pour une compatibilité optimale, nous vous recommandons d&#39;utiliser la fonction de table Iceberg pendant que nous continuons à améliorer la prise en charge du moteur de table Iceberg.
:::

Ce moteur fournit une intégration en lecture seule avec des tables Apache [Iceberg](https://iceberg.apache.org/) existantes dans Amazon S3, Azure, HDFS et stockées localement.

<div id="create-table">
  ## Créer une table
</div>

Notez que la table Iceberg doit déjà exister dans le stockage ; cette commande n&#39;accepte pas de paramètres DDL pour créer une nouvelle table.

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression], [,extra_credentials])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## Arguments du moteur
</div>

La description de ces arguments correspond à celle des arguments des moteurs `S3`, `AzureBlobStorage`, `HDFS` et `File`.
`format` désigne le format des fichiers de données de la table Iceberg.

Pour `IcebergS3`, vous pouvez utiliser le paramètre facultatif `extra_credentials` pour transmettre un `role_arn` afin d&#39;activer un accès basé sur les rôles dans ClickHouse Cloud. Consultez [Sécuriser S3](/fr/cloud/data-sources/secure-s3) pour les étapes de configuration.

Les paramètres du moteur peuvent être spécifiés à l’aide de [collections nommées](../../../operations/named-collections.md)

<div id="example">
  ### Exemple
</div>

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Utiliser les collections nommées :

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

<div id="aliases">
  ## Alias
</div>

Le moteur de table `Iceberg` détecte automatiquement le backend de stockage à partir du paramètre `disk` et sélectionne l’implémentation `IcebergS3`, `IcebergAzure` ou `IcebergLocal` en fonction de celui-ci. Lorsqu’aucun `disk` n’est spécifié, il utilise par défaut l’implémentation `IcebergS3`.

<div id="data-types">
  ## Types de données
</div>

Le tableau suivant montre comment les types de données Iceberg sont associés aux types de données ClickHouse lors de l’inférence du schéma (à des fins de lecture).

<div id="primitive-types">
  ### Types primitifs
</div>

| Type Iceberg       | Type ClickHouse        | Remarques                                                            |
| ------------------ | ---------------------- | -------------------------------------------------------------------- |
| `boolean`          | `Bool`                 |                                                                      |
| `int`              | `Int32`                |                                                                      |
| `long`, `bigint`   | `Int64`                |                                                                      |
| `float`            | `Float32`              |                                                                      |
| `double`           | `Float64`              |                                                                      |
| `date`             | `Date32`               |                                                                      |
| `time`             | `Int64`                | Microsecondes depuis minuit                                          |
| `timestamp`        | `DateTime64(6)`        | Microsecondes, sans fuseau horaire                                   |
| `timestamptz`      | `DateTime64(6, 'UTC')` | Microsecondes, fuseau horaire UTC                                    |
| `timestamp_ns`     | `DateTime64(9)`        | Nanosecondes, sans fuseau horaire (uniquement à partir d’Iceberg v3) |
| `timestamptz_ns`   | `DateTime64(9, 'UTC')` | Nanosecondes, fuseau horaire UTC (uniquement à partir d’Iceberg v3)  |
| `string`, `binary` | `String`               |                                                                      |
| `uuid`             | `UUID`                 |                                                                      |
| `fixed(N)`         | `FixedString(N)`       |                                                                      |
| `decimal(P, S)`    | `Decimal(P, S)`        |                                                                      |

<div id="complex-types">
  ### Types composés
</div>

| Type Iceberg | Type ClickHouse |
| ------------ | --------------- |
| `list`       | `Array`         |
| `map`        | `Map`           |
| `struct`     | `Tuple`         |

<div id="schema-evolution">
  ## Évolution du schéma
</div>

ClickHouse prend en charge la lecture de tables Iceberg dont le schéma a évolué au fil du temps. Cela inclut les tables dans lesquelles des colonnes ont été ajoutées, supprimées ou réordonnées, ainsi que les colonnes passées de required à Nullable. En outre, les conversions de type suivantes sont prises en charge :

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) where P&#39; &gt; P.

À l&#39;heure actuelle, il n&#39;est pas possible de modifier les structures imbriquées ni les types des éléments au sein des Array et des Map.

Pour lire une table dont le schéma a changé après sa création avec l&#39;inférence dynamique du schéma, définissez allow&#95;dynamic&#95;metadata&#95;for&#95;data&#95;lakes = true lors de la création de la table.

<div id="partition-pruning">
  ## Partition pruning
</div>

ClickHouse prend en charge le partition pruning lors des requêtes SELECT sur les tables Iceberg, ce qui contribue à optimiser les performances des requêtes en évitant la lecture de fichiers de données non pertinents. Pour activer le partition pruning, définissez `use_iceberg_partition_pruning = 1`. Pour plus d’informations sur le partition pruning d’Iceberg, consultez https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## Voyage dans le temps
</div>

ClickHouse prend en charge le voyage dans le temps pour les tables Iceberg, ce qui vous permet d&#39;interroger des données historiques à l&#39;aide d&#39;un timestamp ou d&#39;un ID d&#39;instantané donné.

<div id="deleted-rows">
  ## Traitement des tables comportant des lignes supprimées
</div>

ClickHouse prend en charge la lecture des tables Iceberg qui utilisent les méthodes de suppression suivantes :

* [Suppressions par position](https://iceberg.apache.org/spec/#position-delete-files)
* [Suppressions par égalité](https://iceberg.apache.org/spec/#equality-delete-files) (prises en charge à partir de la version 25.8+)

La méthode de suppression suivante n’est **pas prise en charge** :

* [Vecteurs de suppression](https://iceberg.apache.org/spec/#deletion-vectors) (introduits dans la version 3)

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
  ### Points importants
</div>

* **Les instantanés** sont généralement créés lorsque :
  * De nouvelles données sont écrites dans la table
  * Une opération de compaction des données est effectuée

* **Les modifications du schéma ne créent généralement pas d&#39;instantanés** — Cela entraîne des comportements importants lors de l&#39;utilisation du voyage dans le temps avec des tables ayant subi une évolution du schéma.

<div id="example-scenarios">
  ### Exemples de scénarios
</div>

Tous les scénarios sont écrits avec Spark, car CH ne permet pas encore d’écrire dans des tables Iceberg.

<div id="scenario-1">
  #### Scénario 1 : Changements de schéma sans nouveaux snapshots
</div>

Considérez la séquence d’opérations suivante :

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

-- Query the table at each timestamp
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

Résultat de la requête pour différents horodatages :

* À ts1 &amp; ts2 : seules les deux colonnes d’origine apparaissent
* À ts3 : les trois colonnes apparaissent, avec NULL pour le prix de la première ligne

<div id="scenario-2">
  #### Scénario 2 : Différences entre le schéma historique et le schéma actuel
</div>

Une requête de voyage dans le temps exécutée au moment présent peut afficher un schéma différent de celui de la table actuelle :

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number int, 
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

Cela s’explique par le fait que `ALTER TABLE` ne crée pas de nouvel instantané ; pour la table actuelle, Spark récupère la valeur de `schema_id` dans le dernier fichier de métadonnées, et non dans un instantané.

<div id="scenario-3">
  #### Scénario 3 : Différences entre le schéma historique et le schéma actuel
</div>

Le deuxième point, c’est qu’avec le voyage dans le temps, vous ne pouvez pas obtenir l’état d’une table avant que des données y aient été écrites :

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

Dans ClickHouse, le comportement est identique à celui de Spark. Vous pouvez considérer les requêtes Select de Spark comme des requêtes Select de ClickHouse, et cela fonctionnera de la même manière.

<div id="metadata-file-resolution">
  ## Résolution du fichier de métadonnées
</div>

Lors de l&#39;utilisation du moteur de table `Iceberg` dans ClickHouse, le système doit localiser le bon fichier metadata.json, qui décrit la structure de la table Iceberg. Voici comment fonctionne ce processus de résolution :

<div id="candidate-search">
  ### Recherche des fichiers candidats
</div>

1. **Spécification directe du chemin** :

* Si vous définissez `iceberg_metadata_file_path`, le système utilisera exactement ce chemin en le combinant avec le chemin du répertoire de la table Iceberg.
* Lorsque ce paramètre est fourni, tous les autres paramètres de résolution sont ignorés.

2. **Correspondance avec l&#39;UUID de la table** :

* Si `iceberg_metadata_table_uuid` est spécifié, le système :
  * examinera uniquement les fichiers `.metadata.json` du répertoire `metadata`
  * filtrera les fichiers contenant un champ `table-uuid` correspondant à l&#39;UUID spécifié (sans tenir compte de la casse)

3. **Recherche par défaut** :

* Si aucun des paramètres ci-dessus n&#39;est fourni, tous les fichiers `.metadata.json` du répertoire `metadata` sont considérés comme des candidats

<div id="most-recent-file">
  ### Sélection du fichier le plus récent
</div>

Après avoir identifié les fichiers candidats à l’aide des règles ci-dessus, le système détermine lequel est le plus récent :

* Si `iceberg_recent_metadata_file_by_last_updated_ms_field` est activé :
  * Le fichier dont la valeur `last-updated-ms` est la plus élevée est sélectionné

* Sinon :
  * Le fichier dont le numéro de version est le plus élevé est sélectionné
  * (La version apparaît sous la forme `V` dans les noms de fichiers au format `V.metadata.json` ou `V-uuid.metadata.json`)

**Remarque** : Tous les paramètres mentionnés (sauf indication explicite contraire) sont des paramètres du moteur et doivent être spécifiés lors de la création de la table, comme indiqué ci-dessous :

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**Remarque** : Bien que les catalogs Iceberg gèrent généralement la résolution des métadonnées, le moteur de table `Iceberg` de ClickHouse interprète directement les fichiers stockés dans S3 comme des tables Iceberg, d’où l’importance de comprendre ces règles de résolution.

<div id="data-cache">
  ## Cache de données
</div>

Le moteur de table et la fonction de table `Iceberg` prennent en charge la mise en cache des données, au même titre que les stockages `S3`, `AzureBlobStorage` et `HDFS`. Voir [ici](../../../engines/table-engines/integrations/s3.md#data-cache).

<div id="metadata-cache">
  ## Cache de métadonnées
</div>

Le moteur de table `Iceberg` et la fonction de table prennent en charge un cache de métadonnées qui stocke les informations des fichiers manifest, de la manifest list et du fichier JSON de métadonnées. Le cache est stocké en mémoire. Cette fonctionnalité est contrôlée par le paramètre `use_iceberg_metadata_files_cache`, qui est activé par défaut.

<div id="async-metadata-prefetch">
  ## Préchargement asynchrone des métadonnées
</div>

Le préchargement asynchrone des métadonnées peut être activé lors de la création d’une table `Iceberg` en définissant `iceberg_metadata_async_prefetch_period_ms`. Si cette valeur est définie sur 0 (par défaut), ou si le cache de métadonnées n’est pas activé, le préchargement asynchrone est désactivé.
Pour activer cette fonctionnalité, vous devez fournir une valeur non nulle en millisecondes. Elle représente l’intervalle entre les cycles de préchargement.

S’il est activé, le serveur exécutera une opération récurrente en arrière-plan pour parcourir le catalogue distant et détecter les nouvelles versions des métadonnées. Il analysera ensuite ces métadonnées et parcourra récursivement l’instantané, en récupérant les fichiers de liste de manifestes actifs ainsi que les fichiers manifestes.
Les fichiers déjà disponibles dans le cache de métadonnées ne seront pas téléchargés de nouveau. À la fin de chaque cycle de préchargement, le dernier instantané de métadonnées est disponible dans le cache de métadonnées.

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS
    iceberg_metadata_async_prefetch_period_ms = 60000;
```

Afin de tirer le meilleur parti du préchargement asynchrone des métadonnées lors des opérations de lecture, le paramètre `iceberg_metadata_staleness_ms` doit être spécifié en tant que paramètre de requête ou de session. Par défaut (0 - non spécifié), pour chaque requête, le serveur récupère les métadonnées les plus récentes auprès du catalogue distant.
En définissant une tolérance à l’ancienneté des métadonnées, le serveur est autorisé à utiliser la version en cache de l’instantané des métadonnées sans interroger le catalogue distant. S&#39;il existe une version des métadonnées dans le cache et qu’elle a été téléchargée dans la fenêtre d’ancienneté indiquée, elle sera utilisée pour traiter la requête.
Sinon, la version la plus récente sera récupérée depuis le catalogue distant.

```sql
SELECT count() FROM icebench_table WHERE ...
SETTINGS iceberg_metadata_staleness_ms=120000
```

**Remarque** : Le préchargement asynchrone des métadonnées s’exécute dans `ICEBERG_SCEDULE_POOL`, le threadpool côté serveur dédié aux opérations en arrière-plan sur les tables `Iceberg` actives. La taille de ce threadpool est contrôlée par le paramètre de configuration du serveur `iceberg_background_schedule_pool_size` (10 par défaut).

**Remarque** : À ce stade, on part du principe que la taille du cache de métadonnées est suffisante pour contenir intégralement l’instantané de métadonnées le plus récent de toutes les tables actives, si le préchargement asynchrone est activé.

<div id="see-also">
  ## Voir aussi
</div>

* [fonction de table Iceberg](/fr/sql-reference/table-functions/iceberg.md)