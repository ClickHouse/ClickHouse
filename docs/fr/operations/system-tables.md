---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "Les Tables Syst\xE8me"
---

# Les Tables Système {#system-tables}

Les tables système sont utilisées pour implémenter une partie des fonctionnalités du système et pour fournir un accès à des informations sur le fonctionnement du système.
Vous ne pouvez pas supprimer une table système (mais vous pouvez effectuer un détachement).
Les tables système n'ont pas de fichiers avec des données sur le disque ou de fichiers avec des métadonnées. Le serveur crée toutes les tables système au démarrage.
Les tables système sont en lecture seule.
Ils sont situés dans la ‘system’ la base de données.

## système.asynchronous\_metrics {#system_tables-asynchronous_metrics}

Contient des mesures qui sont calculées périodiquement en arrière-plan. Par exemple, la quantité de RAM utilisée.

Colonne:

-   `metric` ([Chaîne](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Float64](../sql-reference/data-types/float.md)) — Metric value.

**Exemple**

``` sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

``` text
┌─metric──────────────────────────────────┬──────value─┐
│ jemalloc.background_thread.run_interval │          0 │
│ jemalloc.background_thread.num_runs     │          0 │
│ jemalloc.background_thread.num_threads  │          0 │
│ jemalloc.retained                       │  422551552 │
│ jemalloc.mapped                         │ 1682989056 │
│ jemalloc.resident                       │ 1656446976 │
│ jemalloc.metadata_thp                   │          0 │
│ jemalloc.metadata                       │   10226856 │
│ UncompressedCacheCells                  │          0 │
│ MarkCacheFiles                          │          0 │
└─────────────────────────────────────────┴────────────┘
```

**Voir Aussi**

-   [Surveiller](monitoring.md) — Base concepts of ClickHouse monitoring.
-   [système.métrique](#system_tables-metrics) — Contains instantly calculated metrics.
-   [système.événement](#system_tables-events) — Contains a number of events that have occurred.
-   [système.metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.

## système.cluster {#system-clusters}

Contient des informations sur les clusters disponibles dans le fichier de configuration et les serveurs en eux.

Colonne:

-   `cluster` (String) — The cluster name.
-   `shard_num` (UInt32) — The shard number in the cluster, starting from 1.
-   `shard_weight` (UInt32) — The relative weight of the shard when writing data.
-   `replica_num` (UInt32) — The replica number in the shard, starting from 1.
-   `host_name` (String) — The host name, as specified in the config.
-   `host_address` (String) — The host IP address obtained from DNS.
-   `port` (UInt16) — The port to use for connecting to the server.
-   `user` (String) — The name of the user for connecting to the server.
-   `errors_count` (UInt32) - nombre de fois que cet hôte n'a pas atteint le réplica.
-   `estimated_recovery_time` (UInt32) - secondes restantes jusqu'à ce que le nombre d'erreurs de réplique soit remis à zéro et qu'il soit considéré comme revenu à la normale.

Veuillez noter que `errors_count` est mise à jour une fois par requête à la grappe, mais `estimated_recovery_time` est recalculé sur-demande. Il pourrait donc y avoir un cas de non-zéro `errors_count` et zéro `estimated_recovery_time`, cette requête suivante sera nulle `errors_count` et essayez d'utiliser des répliques comme si elle ne comporte pas d'erreurs.

**Voir aussi**

-   [Tableau moteur Distribués](../engines/table-engines/special/distributed.md)
-   [paramètre distributed\_replica\_error\_cap](settings/settings.md#settings-distributed_replica_error_cap)
-   [paramètre distributed\_replica\_error\_half\_life](settings/settings.md#settings-distributed_replica_error_half_life)

## système.colonne {#system-columns}

Contient des informations sur les colonnes de toutes les tables.

Vous pouvez utiliser ce tableau pour obtenir des informations similaires à l' [DESCRIBE TABLE](../sql-reference/statements/misc.md#misc-describe-table) requête, mais pour plusieurs tables à la fois.

Le `system.columns` le tableau contient les colonnes suivantes (la colonne type est indiqué entre parenthèses):

-   `database` (String) — Database name.
-   `table` (String) — Table name.
-   `name` (String) — Column name.
-   `type` (String) — Column type.
-   `default_kind` (String) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`) pour la valeur par défaut, ou une chaîne vide si elle n'est pas définie.
-   `default_expression` (String) — Expression for the default value, or an empty string if it is not defined.
-   `data_compressed_bytes` (UInt64) — The size of compressed data, in bytes.
-   `data_uncompressed_bytes` (UInt64) — The size of decompressed data, in bytes.
-   `marks_bytes` (UInt64) — The size of marks, in bytes.
-   `comment` (String) — Comment on the column, or an empty string if it is not defined.
-   `is_in_partition_key` (UInt8) — Flag that indicates whether the column is in the partition expression.
-   `is_in_sorting_key` (UInt8) — Flag that indicates whether the column is in the sorting key expression.
-   `is_in_primary_key` (UInt8) — Flag that indicates whether the column is in the primary key expression.
-   `is_in_sampling_key` (UInt8) — Flag that indicates whether the column is in the sampling key expression.

## système.contributeur {#system-contributors}

Contient des informations sur les donateurs. Tous les constributors dans un ordre aléatoire. L'ordre est aléatoire au moment de l'exécution de la requête.

Colonne:

-   `name` (String) — Contributor (author) name from git log.

**Exemple**

``` sql
SELECT * FROM system.contributors LIMIT 10
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

Trouver vous-même dans le tableau, utilisez une requête:

``` sql
SELECT * FROM system.contributors WHERE name='Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```

## système.les bases de données {#system-databases}

Cette table contient une seule colonne de chaîne appelée ‘name’ – the name of a database.
Chaque base de données que le serveur connaît a une entrée correspondante dans la table.
Cette table système est utilisée pour implémenter `SHOW DATABASES` requête.

## système.detached\_parts {#system_tables-detached_parts}

Contient des informations sur les pièces détachées de [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) table. Le `reason` colonne spécifie pourquoi la pièce a été détachée. Pour les pièces détachées par l'utilisateur, la raison est vide. De telles pièces peuvent être attachées avec [ALTER TABLE ATTACH PARTITION\|PART](../sql-reference/statements/alter.md#alter_attach-partition) commande. Pour la description des autres colonnes, voir [système.partie](#system_tables-parts). Si le nom de pièce n'est pas valide, les valeurs de certaines colonnes peuvent être `NULL`. Ces pièces peuvent être supprimés avec [ALTER TABLE DROP DETACHED PART](../sql-reference/statements/alter.md#alter_drop-detached).

## système.dictionnaire {#system_tables-dictionaries}

Contient des informations sur [dictionnaires externes](../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

Colonne:

-   `database` ([Chaîne](../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
-   `name` ([Chaîne](../sql-reference/data-types/string.md)) — [Nom du dictionnaire](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([Enum8](../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    -   `NOT_LOADED` — Dictionary was not loaded because it was not used.
    -   `LOADED` — Dictionary loaded successfully.
    -   `FAILED` — Unable to load the dictionary as a result of an error.
    -   `LOADING` — Dictionary is loading now.
    -   `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](../sql-reference/statements/system.md#query_language-system-reload-dictionary) requête, délai d'attente, configuration du dictionnaire a changé).
    -   `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
-   `origin` ([Chaîne](../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
-   `type` ([Chaîne](../sql-reference/data-types/string.md)) — Type of a dictionary allocation. [Stockage des dictionnaires en mémoire](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [Type de clé](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key): Touche Numérique ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) or Сomposite key ([Chaîne](../sql-reference/data-types/string.md)) — form “(type 1, type 2, …, type n)”.
-   `attribute.names` ([Tableau](../sql-reference/data-types/array.md)([Chaîne](../sql-reference/data-types/string.md))) — Array of [les noms d'attribut](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) fournis par le dictionnaire.
-   `attribute.types` ([Tableau](../sql-reference/data-types/array.md)([Chaîne](../sql-reference/data-types/string.md))) — Corresponding array of [les types d'attribut](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) qui sont fournis par le dictionnaire.
-   `bytes_allocated` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Amount of RAM allocated for the dictionary.
-   `query_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
-   `hit_rate` ([Float64](../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
-   `element_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of items stored in the dictionary.
-   `load_factor` ([Float64](../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
-   `source` ([Chaîne](../sql-reference/data-types/string.md)) — Text describing the [source de données](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) pour le dictionnaire.
-   `lifetime_min` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Minimum [vie](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) du dictionnaire en mémoire, après quoi ClickHouse tente de recharger le dictionnaire (si `invalidate_query` est définie, alors que si elle a changé). Réglez en quelques secondes.
-   `lifetime_max` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Maximum [vie](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) du dictionnaire en mémoire, après quoi ClickHouse tente de recharger le dictionnaire (si `invalidate_query` est définie, alors que si elle a changé). Réglez en quelques secondes.
-   `loading_start_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
-   `last_successful_update_time` ([DateTime](../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with external sources and investigate causes.
-   `loading_duration` ([Float32](../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
-   `last_exception` ([Chaîne](../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn't be created.

**Exemple**

Configurez le dictionnaire.

``` sql
CREATE DICTIONARY dictdb.dict
(
    `key` Int64 DEFAULT -1,
    `value_default` String DEFAULT 'world',
    `value_expression` String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())
```

Assurez-vous que le dictionnaire est chargé.

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```

## système.événement {#system_tables-events}

Contient des informations sur le nombre d'événements survenus dans le système. Par exemple, dans le tableau, vous pouvez trouver combien `SELECT` les requêtes ont été traitées depuis le démarrage du serveur ClickHouse.

Colonne:

-   `event` ([Chaîne](../sql-reference/data-types/string.md)) — Event name.
-   `value` ([UInt64](../sql-reference/data-types/int-uint.md)) — Number of events occurred.
-   `description` ([Chaîne](../sql-reference/data-types/string.md)) — Event description.

**Exemple**

``` sql
SELECT * FROM system.events LIMIT 5
```

``` text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.                  │
│ SelectQuery                           │     8 │ Same as Query, but only for SELECT queries.                                                                                                                                                                                                                │
│ FileOpen                              │    73 │ Number of files opened.                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ Number of reads (read/pread) from a file descriptor. Does not include sockets.                                                                                                                                                                             │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.                                                                                                                                              │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Voir Aussi**

-   [système.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [système.métrique](#system_tables-metrics) — Contains instantly calculated metrics.
-   [système.metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [Surveiller](monitoring.md) — Base concepts of ClickHouse monitoring.

## système.fonction {#system-functions}

Contient des informations sur les fonctions normales et agrégées.

Colonne:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

## système.graphite\_retentions {#system-graphite-retentions}

Contient des informations sur les paramètres [graphite\_rollup](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) qui sont utilisés dans les tableaux avec [\* GraphiteMergeTree](../engines/table-engines/mergetree-family/graphitemergetree.md) moteur.

Colonne:

-   `config_name` (Chaîne) - `graphite_rollup` nom du paramètre.
-   `regexp` (Chaîne) - un modèle pour le nom de la métrique.
-   `function` (Chaîne) - le nom de la fonction d'agrégation.
-   `age` (UInt64) - l'âge minimum des données en secondes.
-   `precision` (UInt64) - comment définir précisément l'âge des données en secondes.
-   `priority` (UInt16) - priorité de motif.
-   `is_default` (UInt8) - indique si le motif est la valeur par défaut.
-   `Tables.database` (Array (String)) - tableau de noms de tables de base de données qui utilisent `config_name` paramètre.
-   `Tables.table` (Array (String)) - tableau de noms de tables qui utilisent `config_name` paramètre.

## système.fusionner {#system-merges}

Contient des informations sur les fusions et les mutations de pièces actuellement en cours pour les tables de la famille MergeTree.

Colonne:

-   `database` (String) — The name of the database the table is in.
-   `table` (String) — Table name.
-   `elapsed` (Float64) — The time elapsed (in seconds) since the merge started.
-   `progress` (Float64) — The percentage of completed work from 0 to 1.
-   `num_parts` (UInt64) — The number of pieces to be merged.
-   `result_part_name` (String) — The name of the part that will be formed as the result of merging.
-   `is_mutation` (UInt8) - 1 si ce processus est une mutation partielle.
-   `total_size_bytes_compressed` (UInt64) — The total size of the compressed data in the merged chunks.
-   `total_size_marks` (UInt64) — The total number of marks in the merged parts.
-   `bytes_read_uncompressed` (UInt64) — Number of bytes read, uncompressed.
-   `rows_read` (UInt64) — Number of rows read.
-   `bytes_written_uncompressed` (UInt64) — Number of bytes written, uncompressed.
-   `rows_written` (UInt64) — Number of rows written.

## système.métrique {#system_tables-metrics}

Contient des mesures qui peuvent être calculées instantanément, ou ont une valeur actuelle. Par exemple, le nombre de requêtes traitées ou en cours réplique de retard. Ce tableau est toujours à jour.

Colonne:

-   `metric` ([Chaîne](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Int64](../sql-reference/data-types/int-uint.md)) — Metric value.
-   `description` ([Chaîne](../sql-reference/data-types/string.md)) — Metric description.

La liste des mesures que vous pouvez trouver dans le [src / Common / CurrentMetrics.rpc](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) fichier source de ClickHouse.

**Exemple**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric─────────────────────┬─value─┬─description──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                      │     1 │ Number of executing queries                                                                                                                                                                      │
│ Merge                      │     0 │ Number of executing background merges                                                                                                                                                            │
│ PartMutation               │     0 │ Number of mutations (ALTER DELETE/UPDATE)                                                                                                                                                        │
│ ReplicatedFetch            │     0 │ Number of data parts being fetched from replicas                                                                                                                                                │
│ ReplicatedSend             │     0 │ Number of data parts being sent to replicas                                                                                                                                                      │
│ ReplicatedChecks           │     0 │ Number of data parts checking for consistency                                                                                                                                                    │
│ BackgroundPoolTask         │     0 │ Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches, or replication queue bookkeeping)                                                                                │
│ BackgroundSchedulePoolTask │     0 │ Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.   │
│ DiskSpaceReservedForMerge  │     0 │ Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.                                                                     │
│ DistributedSend            │     0 │ Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.                                                          │
└────────────────────────────┴───────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Voir Aussi**

-   [système.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [système.événement](#system_tables-events) — Contains a number of events that occurred.
-   [système.metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [Surveiller](monitoring.md) — Base concepts of ClickHouse monitoring.

## système.metric\_log {#system_tables-metric_log}

Contient l'historique des valeurs de métriques des tables `system.metrics` et `system.events` périodiquement vidé sur le disque.
Pour activer la collection d'historique des métriques `system.metric_log`, créer `/etc/clickhouse-server/config.d/metric_log.xml` avec le contenu suivant:

``` xml
<yandex>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
    </metric_log>
</yandex>
```

**Exemple**

``` sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
event_date:                                                 2020-02-18
event_time:                                                 2020-02-18 07:15:33
milliseconds:                                               554
ProfileEvent_Query:                                         0
ProfileEvent_SelectQuery:                                   0
ProfileEvent_InsertQuery:                                   0
ProfileEvent_FileOpen:                                      0
ProfileEvent_Seek:                                          0
ProfileEvent_ReadBufferFromFileDescriptorRead:              1
ProfileEvent_ReadBufferFromFileDescriptorReadFailed:        0
ProfileEvent_ReadBufferFromFileDescriptorReadBytes:         0
ProfileEvent_WriteBufferFromFileDescriptorWrite:            1
ProfileEvent_WriteBufferFromFileDescriptorWriteFailed:      0
ProfileEvent_WriteBufferFromFileDescriptorWriteBytes:       56
...
CurrentMetric_Query:                                        0
CurrentMetric_Merge:                                        0
CurrentMetric_PartMutation:                                 0
CurrentMetric_ReplicatedFetch:                              0
CurrentMetric_ReplicatedSend:                               0
CurrentMetric_ReplicatedChecks:                             0
...
```

**Voir aussi**

-   [système.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [système.événement](#system_tables-events) — Contains a number of events that occurred.
-   [système.métrique](#system_tables-metrics) — Contains instantly calculated metrics.
-   [Surveiller](monitoring.md) — Base concepts of ClickHouse monitoring.

## système.nombre {#system-numbers}

Cette table contient une seule colonne UInt64 nommée ‘number’ qui contient presque tous les nombres naturels à partir de zéro.
Vous pouvez utiliser cette table pour les tests, ou si vous avez besoin de faire une recherche de force brute.
Les lectures de cette table ne sont pas parallélisées.

## système.numbers\_mt {#system-numbers-mt}

Le même que ‘system.numbers’ mais les lectures sont parallélisées. Les nombres peuvent être retournés dans n'importe quel ordre.
Utilisé pour les tests.

## système.un {#system-one}

Cette table contient une seule ligne avec un ‘dummy’ Colonne UInt8 contenant la valeur 0.
Cette table est utilisée si une requête SELECT ne spécifie pas la clause FROM.
Ceci est similaire à la table double trouvée dans d'autres SGBD.

## système.partie {#system_tables-parts}

Contient des informations sur les parties de [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) table.

Chaque ligne décrit une partie des données.

Colonne:

-   `partition` (String) – The partition name. To learn what a partition is, see the description of the [ALTER](../sql-reference/statements/alter.md#query_language_queries_alter) requête.

    Format:

    -   `YYYYMM` pour le partitionnement automatique par mois.
    -   `any_string` lors du partitionnement manuel.

-   `name` (`String`) – Name of the data part.

-   `active` (`UInt8`) – Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's deleted. Inactive data parts remain after merging.

-   `marks` (`UInt64`) – The number of marks. To get the approximate number of rows in a data part, multiply `marks` par la granularité d'index (généralement 8192) (cet indice ne fonctionne pas pour la granularité adaptative).

-   `rows` (`UInt64`) – The number of rows.

-   `bytes_on_disk` (`UInt64`) – Total size of all the data part files in bytes.

-   `data_compressed_bytes` (`UInt64`) – Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `data_uncompressed_bytes` (`UInt64`) – Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `marks_bytes` (`UInt64`) – The size of the file with marks.

-   `modification_time` (`DateTime`) – The time the directory with the data part was modified. This usually corresponds to the time of data part creation.\|

-   `remove_time` (`DateTime`) – The time when the data part became inactive.

-   `refcount` (`UInt32`) – The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges.

-   `min_date` (`Date`) – The minimum value of the date key in the data part.

-   `max_date` (`Date`) – The maximum value of the date key in the data part.

-   `min_time` (`DateTime`) – The minimum value of the date and time key in the data part.

-   `max_time`(`DateTime`) – The maximum value of the date and time key in the data part.

-   `partition_id` (`String`) – ID of the partition.

-   `min_block_number` (`UInt64`) – The minimum number of data parts that make up the current part after merging.

-   `max_block_number` (`UInt64`) – The maximum number of data parts that make up the current part after merging.

-   `level` (`UInt32`) – Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts.

-   `data_version` (`UInt64`) – Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than `data_version`).

-   `primary_key_bytes_in_memory` (`UInt64`) – The amount of memory (in bytes) used by primary key values.

-   `primary_key_bytes_in_memory_allocated` (`UInt64`) – The amount of memory (in bytes) reserved for primary key values.

-   `is_frozen` (`UInt8`) – Flag that shows that a partition data backup exists. 1, the backup exists. 0, the backup doesn't exist. For more details, see [FREEZE PARTITION](../sql-reference/statements/alter.md#alter_freeze-partition)

-   `database` (`String`) – Name of the database.

-   `table` (`String`) – Name of the table.

-   `engine` (`String`) – Name of the table engine without parameters.

-   `path` (`String`) – Absolute path to the folder with data part files.

-   `disk` (`String`) – Name of a disk that stores the data part.

-   `hash_of_all_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) de fichiers compressés.

-   `hash_of_uncompressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) de fichiers non compressés (fichiers avec des marques, fichier d'index, etc.).

-   `uncompressed_hash_of_compressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) des données dans les fichiers compressés comme s'ils étaient non compressé.

-   `bytes` (`UInt64`) – Alias for `bytes_on_disk`.

-   `marks_size` (`UInt64`) – Alias for `marks_bytes`.

## système.part\_log {#system_tables-part-log}

Le `system.part_log` la table est créée uniquement si [part\_log](server-configuration-parameters/settings.md#server_configuration_parameters-part-log) serveur paramètre est spécifié.

Ce tableau contient des informations sur les événements survenus avec [les parties de données](../engines/table-engines/mergetree-family/custom-partitioning-key.md) dans le [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) table de famille, telles que l'ajout ou la fusion de données.

Le `system.part_log` le tableau contient les colonnes suivantes:

-   `event_type` (Enum) — Type of the event that occurred with the data part. Can have one of the following values:
    -   `NEW_PART` — Inserting of a new data part.
    -   `MERGE_PARTS` — Merging of data parts.
    -   `DOWNLOAD_PART` — Downloading a data part.
    -   `REMOVE_PART` — Removing or detaching a data part using [DETACH PARTITION](../sql-reference/statements/alter.md#alter_detach-partition).
    -   `MUTATE_PART` — Mutating of a data part.
    -   `MOVE_PART` — Moving the data part from the one disk to another one.
-   `event_date` (Date) — Event date.
-   `event_time` (DateTime) — Event time.
-   `duration_ms` (UInt64) — Duration.
-   `database` (String) — Name of the database the data part is in.
-   `table` (String) — Name of the table the data part is in.
-   `part_name` (String) — Name of the data part.
-   `partition_id` (String) — ID of the partition that the data part was inserted to. The column takes the ‘all’ si le partitionnement est par `tuple()`.
-   `rows` (UInt64) — The number of rows in the data part.
-   `size_in_bytes` (UInt64) — Size of the data part in bytes.
-   `merged_from` (Array(String)) — An array of names of the parts which the current part was made up from (after the merge).
-   `bytes_uncompressed` (UInt64) — Size of uncompressed bytes.
-   `read_rows` (UInt64) — The number of rows was read during the merge.
-   `read_bytes` (UInt64) — The number of bytes was read during the merge.
-   `error` (UInt16) — The code number of the occurred error.
-   `exception` (String) — Text message of the occurred error.

Le `system.part_log` la table est créée après la première insertion de données `MergeTree` table.

## système.processus {#system_tables-processes}

Cette table système est utilisée pour implémenter `SHOW PROCESSLIST` requête.

Colonne:

-   `user` (String) – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the `default` utilisateur. Le champ contient le nom d'utilisateur pour une requête spécifique, pas pour une requête que cette requête lancée.
-   `address` (String) – The IP address the request was made from. The same for distributed processing. To track where a distributed query was originally made from, look at `system.processes` sur le serveur du demandeur de requête.
-   `elapsed` (Float64) – The time in seconds since request execution started.
-   `rows_read` (UInt64) – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `bytes_read` (UInt64) – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `total_rows_approx` (UInt64) – The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.
-   `memory_usage` (UInt64) – Amount of RAM the request uses. It might not include some types of dedicated memory. See the [max\_memory\_usage](../operations/settings/query-complexity.md#settings_max_memory_usage) paramètre.
-   `query` (String) – The query text. For `INSERT` il n'inclut pas les données à insérer.
-   `query_id` (String) – Query ID, if defined.

## système.text\_log {#system-tables-text-log}

Contient des entrées de journalisation. Niveau de journalisation qui va à cette table peut être limité `text_log.level` paramètre de serveur.

Colonne:

-   `event_date` (`Date`) - Date de l'entrée.
-   `event_time` (`DateTime`) - Temps de l'entrée.
-   `microseconds` (`UInt32`) - Microsecondes de l'entrée.
-   `thread_name` (String) — Name of the thread from which the logging was done.
-   `thread_id` (UInt64) — OS thread ID.
-   `level` (`Enum8`) - Niveau d'entrée.
    -   `'Fatal' = 1`
    -   `'Critical' = 2`
    -   `'Error' = 3`
    -   `'Warning' = 4`
    -   `'Notice' = 5`
    -   `'Information' = 6`
    -   `'Debug' = 7`
    -   `'Trace' = 8`
-   `query_id` (`String`)- ID de la requête.
-   `logger_name` (`LowCardinality(String)`) - Name of the logger (i.e. `DDLWorker`)
-   `message` (`String`) - Le message lui-même.
-   `revision` (`UInt32`)- Révision ClickHouse.
-   `source_file` (`LowCardinality(String)`)- Fichier Source à partir duquel la journalisation a été effectuée.
-   `source_line` (`UInt64`)- Ligne Source à partir de laquelle la journalisation a été effectuée.

## système.query\_log {#system_tables-query_log}

Contient des informations sur l'exécution de requêtes. Pour chaque requête, vous pouvez voir l'Heure de début du traitement, la durée du traitement, les messages d'erreur et d'autres informations.

!!! note "Note"
    Le tableau ne contient pas les données d'entrée pour `INSERT` requête.

Clickhouse crée cette table uniquement si [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) serveur paramètre est spécifié. Ce paramètre définit les règles de journalisation, tels que l'intervalle d'enregistrement ou le nom de la table, la requête sera connecté.

Pour activer la journalisation des requêtes, définissez [log\_queries](settings/settings.md#settings-log-queries) paramètre 1. Pour plus de détails, voir le [Paramètre](settings/settings.md) section.

Le `system.query_log` table enregistre deux types de requêtes:

1.  Requêtes initiales qui ont été exécutées directement par le client.
2.  Requêtes enfants initiées par d'autres requêtes (pour l'exécution de requêtes distribuées). Pour ces types de requêtes, des informations sur les requêtes parentes sont affichées dans `initial_*` colonne.

Colonne:

-   `type` (`Enum8`) — Type of event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` (Date) — Query starting date.
-   `event_time` (DateTime) — Query starting time.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` des requêtes, le nombre de lignes. Pour les autres requêtes, la valeur de la colonne est 0.
-   `written_bytes` (UInt64) — For `INSERT` des requêtes, le nombre d'octets écrits. Pour les autres requêtes, la valeur de la colonne est 0.
-   `result_rows` (UInt64) — Number of rows in the result.
-   `result_bytes` (UInt64) — Number of bytes in the result.
-   `memory_usage` (UInt64) — Memory consumption by the query.
-   `query` (String) — Query string.
-   `exception` (String) — Exception message.
-   `stack_trace` (String) — Stack trace (a list of methods called before the error occurred). An empty string, if the query is completed successfully.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [clickhouse-client](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [clickhouse-client](../interfaces/cli.md) ou un autre client TCP est exécuté.
-   `client_name` (String) — The [clickhouse-client](../interfaces/cli.md) ou un autre nom de client TCP.
-   `client_revision` (UInt32) — Revision of the [clickhouse-client](../interfaces/cli.md) ou un autre client TCP.
-   `client_version_major` (UInt32) — Major version of the [clickhouse-client](../interfaces/cli.md) ou un autre client TCP.
-   `client_version_minor` (UInt32) — Minor version of the [clickhouse-client](../interfaces/cli.md) ou un autre client TCP.
-   `client_version_patch` (UInt32) — Patch component of the [clickhouse-client](../interfaces/cli.md) ou une autre version du client TCP.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` la méthode a été utilisée.
    -   2 — `POST` la méthode a été utilisée.
-   `http_user_agent` (String) — The `UserAgent` en-tête passé dans la requête HTTP.
-   `quota_key` (String) — The “quota key” spécifié dans le [quota](quotas.md) (voir `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `thread_numbers` (Array(UInt32)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics. The description of them could be found in the table [système.événement](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics that are listed in the `ProfileEvents.Names` colonne.
-   `Settings.Names` (Array(String)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` paramètre 1.
-   `Settings.Values` (Array(String)) — Values of settings that are listed in the `Settings.Names` colonne.

Chaque requête crée une ou deux lignes dans le `query_log` le tableau, en fonction de l'état de la requête:

1.  Si l'exécution de la requête est réussie, deux événements de type 1 et 2 sont créés (voir `type` colonne).
2.  Si une erreur s'est produite pendant le traitement de la requête, deux événements avec les types 1 et 4 sont créés.
3.  Si une erreur s'est produite avant le lancement de la requête, un seul événement de type 3 est créé.

Par défaut, les journaux sont ajoutés à la table à des intervalles de 7,5 secondes. Vous pouvez définir cet intervalle dans la [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) configuration du serveur (voir `flush_interval_milliseconds` paramètre). Pour vider les journaux de force du tampon mémoire dans la table, utilisez le `SYSTEM FLUSH LOGS` requête.

Lorsque la table est supprimée manuellement, il sera automatiquement créé à la volée. Notez que tous les précédents journaux seront supprimés.

!!! note "Note"
    La période de stockage des journaux est illimitée. Les journaux ne sont pas automatiquement supprimés de la table. Vous devez organiser vous-même la suppression des journaux obsolètes.

Vous pouvez spécifier une clé de partitionnement arbitraire pour `system.query_log` la table dans le [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) configuration du serveur (voir `partition_by` paramètre).

## système.query\_thread\_log {#system_tables-query-thread-log}

La table contient des informations sur chaque thread d'exécution de requête.

Clickhouse crée cette table uniquement si [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) serveur paramètre est spécifié. Ce paramètre définit les règles de journalisation, tels que l'intervalle d'enregistrement ou le nom de la table, la requête sera connecté.

Pour activer la journalisation des requêtes, définissez [log\_query\_threads](settings/settings.md#settings-log-query-threads) paramètre 1. Pour plus de détails, voir le [Paramètre](settings/settings.md) section.

Colonne:

-   `event_date` (Date) — the date when the thread has finished execution of the query.
-   `event_time` (DateTime) — the date and time when the thread has finished execution of the query.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` des requêtes, le nombre de lignes. Pour les autres requêtes, la valeur de la colonne est 0.
-   `written_bytes` (UInt64) — For `INSERT` des requêtes, le nombre d'octets écrits. Pour les autres requêtes, la valeur de la colonne est 0.
-   `memory_usage` (Int64) — The difference between the amount of allocated and freed memory in context of this thread.
-   `peak_memory_usage` (Int64) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `thread_name` (String) — Name of the thread.
-   `thread_number` (UInt32) — Internal thread ID.
-   `os_thread_id` (Int32) — OS thread ID.
-   `master_thread_id` (UInt64) — OS initial ID of initial thread.
-   `query` (String) — Query string.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [clickhouse-client](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [clickhouse-client](../interfaces/cli.md) ou un autre client TCP est exécuté.
-   `client_name` (String) — The [clickhouse-client](../interfaces/cli.md) ou un autre nom de client TCP.
-   `client_revision` (UInt32) — Revision of the [clickhouse-client](../interfaces/cli.md) ou un autre client TCP.
-   `client_version_major` (UInt32) — Major version of the [clickhouse-client](../interfaces/cli.md) ou un autre client TCP.
-   `client_version_minor` (UInt32) — Minor version of the [clickhouse-client](../interfaces/cli.md) ou un autre client TCP.
-   `client_version_patch` (UInt32) — Patch component of the [clickhouse-client](../interfaces/cli.md) ou une autre version du client TCP.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` la méthode a été utilisée.
    -   2 — `POST` la méthode a été utilisée.
-   `http_user_agent` (String) — The `UserAgent` en-tête passé dans la requête HTTP.
-   `quota_key` (String) — The “quota key” spécifié dans le [quota](quotas.md) (voir `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics for this thread. The description of them could be found in the table [système.événement](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics for this thread that are listed in the `ProfileEvents.Names` colonne.

Par défaut, les journaux sont ajoutés à la table à des intervalles de 7,5 secondes. Vous pouvez définir cet intervalle dans la [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) configuration du serveur (voir `flush_interval_milliseconds` paramètre). Pour vider les journaux de force du tampon mémoire dans la table, utilisez le `SYSTEM FLUSH LOGS` requête.

Lorsque la table est supprimée manuellement, il sera automatiquement créé à la volée. Notez que tous les précédents journaux seront supprimés.

!!! note "Note"
    La période de stockage des journaux est illimitée. Les journaux ne sont pas automatiquement supprimés de la table. Vous devez organiser vous-même la suppression des journaux obsolètes.

Vous pouvez spécifier une clé de partitionnement arbitraire pour `system.query_thread_log` la table dans le [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) configuration du serveur (voir `partition_by` paramètre).

## système.trace\_log {#system_tables-trace_log}

Contient des traces de pile collectées par le profileur de requête d'échantillonnage.

Clickhouse crée cette table lorsque le [trace\_log](server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) la section de configuration du serveur est définie. Aussi l' [query\_profiler\_real\_time\_period\_ns](settings/settings.md#query_profiler_real_time_period_ns) et [query\_profiler\_cpu\_time\_period\_ns](settings/settings.md#query_profiler_cpu_time_period_ns) paramètres doivent être définis.

Pour analyser les journaux, utilisez `addressToLine`, `addressToSymbol` et `demangle` fonctions d'introspection.

Colonne:

-   `event_date` ([Date](../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `timestamp_ns` ([UInt64](../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    Lors de la connexion au serveur par `clickhouse-client`, vous voyez la chaîne similaire à `Connected to ClickHouse server version 19.18.1 revision 54429.`. Ce champ contient le `revision` mais pas le `version` d'un serveur.

-   `timer_type` ([Enum8](../sql-reference/data-types/enum.md)) — Timer type:

    -   `Real` représente l'horloge murale.
    -   `CPU` représente le temps CPU.

-   `thread_number` ([UInt32](../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([Chaîne](../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query\_log](#system_tables-query_log) système de table.

-   `trace` ([Tableau (UInt64)](../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**Exemple**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-15
event_time:    2019-11-15 15:09:38
revision:      54428
timer_type:    Real
thread_number: 48
query_id:      acc4d61f-5bd1-4a3e-bc91-2180be37c915
trace:         [94222141367858,94222152240175,94222152325351,94222152329944,94222152330796,94222151449980,94222144088167,94222151682763,94222144088167,94222151682763,94222144088167,94222144058283,94222144059248,94222091840750,94222091842302,94222091831228,94222189631488,140509950166747,140509942945935]
```

## système.réplique {#system_tables-replicas}

Contient des informations et l'état des tables répliquées résidant sur le serveur local.
Ce tableau peut être utilisé pour la surveillance. La table contient une ligne pour chaque Répliqué\* table.

Exemple:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

``` text
Row 1:
──────
database:                   merge
table:                      visits
engine:                     ReplicatedCollapsingMergeTree
is_leader:                  1
can_become_leader:          1
is_readonly:                0
is_session_expired:         0
future_parts:               1
parts_to_check:             0
zookeeper_path:             /clickhouse/tables/01-06/visits
replica_name:               example01-06-1.yandex.ru
replica_path:               /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:            9
queue_size:                 1
inserts_in_queue:           0
merges_in_queue:            1
part_mutations_in_queue:    0
queue_oldest_time:          2020-02-20 08:34:30
inserts_oldest_time:        1970-01-01 00:00:00
merges_oldest_time:         2020-02-20 08:34:30
part_mutations_oldest_time: 1970-01-01 00:00:00
oldest_part_to_get:
oldest_part_to_merge_to:    20200220_20284_20840_7
oldest_part_to_mutate_to:
log_max_index:              596273
log_pointer:                596274
last_queue_update:          2020-02-20 08:34:32
absolute_delay:             0
total_replicas:             2
active_replicas:            2
```

Colonne:

-   `database` (`String`) - Nom de base de données
-   `table` (`String`)- Nom de la Table
-   `engine` (`String`)- Nom du moteur de Table
-   `is_leader` (`UInt8`) - Si la réplique est le chef de file.
    Une seule réplique à la fois peut être le leader. Le leader est responsable de la sélection des fusions d'arrière-plan à effectuer.
    Notez que les Écritures peuvent être effectuées sur n'importe quel réplica disponible et ayant une session dans ZK, qu'il s'agisse d'un leader.
-   `can_become_leader` (`UInt8`)- Si la réplique peut être élue en tant que leader.
-   `is_readonly` (`UInt8`) - Si la réplique est en mode lecture seule.
    Ce mode est activé si la configuration n'a pas de sections avec ZooKeeper, si une erreur inconnue s'est produite lors de la réinitialisation des sessions dans ZooKeeper et lors de la réinitialisation des sessions dans ZooKeeper.
-   `is_session_expired` (`UInt8`)- la session avec ZooKeeper a expiré. Fondamentalement le même que `is_readonly`.
-   `future_parts` (`UInt32`)- Le nombre de parties de données qui apparaîtront à la suite D'insertions ou de fusions qui n'ont pas encore été effectuées.
-   `parts_to_check` (`UInt32`) - Le nombre de parties des données dans la file d'attente pour la vérification. Une pièce est placée dans la file d'attente de vérification s'il y a un soupçon qu'elle pourrait être endommagée.
-   `zookeeper_path` (`String`)- Chemin d'accès aux données de la table dans ZooKeeper.
-   `replica_name` (`String`) - Réplique nom de la Gardienne. Différentes répliques d'une même table ont des noms différents.
-   `replica_path` (`String`)- Chemin vers les données de réplique dans ZooKeeper. La même chose que la concaténation ‘zookeeper\_path/replicas/replica\_path’.
-   `columns_version` (`Int32`)- Numéro de Version de la structure de la table. Indique combien de fois ALTER a été effectué. Si les répliques ont des versions différentes, cela signifie que certaines répliques n'ont pas encore Toutes les modifications.
-   `queue_size` (`UInt32`),- La taille de la file d'attente pour les opérations en attente d'être exécuté. Les opérations comprennent l'insertion de blocs de données, les fusions et certaines autres actions. Il coïncide généralement avec `future_parts`.
-   `inserts_in_queue` (`UInt32`) - Nombre d'insertions de blocs de données qui doivent être faits. Les Insertions sont généralement répliquées assez rapidement. Si ce nombre est grand, cela signifie que quelque chose est faux.
-   `merges_in_queue` (`UInt32`) - Le nombre de fusions en attente d'être fait. Parfois, les fusions sont longues, donc cette valeur peut être supérieure à zéro pendant une longue période.
-   `part_mutations_in_queue` (`UInt32`) - Le nombre de mutations en attente d'être fait.
-   `queue_oldest_time` (`DateTime`) - Si `queue_size` supérieur à 0, indique quand l'opération la plus ancienne a été ajoutée à la file d'attente.
-   `inserts_oldest_time` (`DateTime`) - Voir `queue_oldest_time`
-   `merges_oldest_time` (`DateTime`) - Voir `queue_oldest_time`
-   `part_mutations_oldest_time` (`DateTime`) - Voir `queue_oldest_time`

Les 4 colonnes suivantes ont une valeur non nulle uniquement lorsqu'il y a une session active avec ZK.

-   `log_max_index` (`UInt64`) - Maximum nombre d'entrées dans le journal de l'activité générale.
-   `log_pointer` (`UInt64`)- Numéro d'entrée Maximum dans le journal de l'activité générale que le réplica a copié dans sa file d'attente d'exécution, plus un. Si `log_pointer` est beaucoup plus petite que `log_max_index` quelque chose ne va pas.
-   `last_queue_update` (`DateTime`) - Lorsque la file d'attente a été mise à jour la dernière fois.
-   `absolute_delay` (`UInt64`)- Combien de décalage en secondes la réplique actuelle A.
-   `total_replicas` (`UInt8`) - Le nombre total de répliques connues de ce tableau.
-   `active_replicas` (`UInt8`) - Le nombre de répliques de cette table qui ont une session dans ZooKeeper (c'est-à-dire le nombre de répliques fonctionnelles).

Si vous demandez toutes les colonnes, la table peut fonctionner un peu lentement, car plusieurs lectures de ZooKeeper sont faites pour chaque ligne.
Si vous ne demandez pas les 4 dernières colonnes (log\_max\_index, log\_pointer, total\_replicas, active\_replicas), la table fonctionne rapidement.

Par exemple, vous pouvez vérifier que tout fonctionne correctement comme ceci:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

Si cette requête ne retourne rien, cela signifie que tout va bien.

## système.paramètre {#system-tables-system-settings}

Contient des informations sur les paramètres de session pour l'utilisateur actuel.

Colonne:

-   `name` ([Chaîne](../sql-reference/data-types/string.md)) — Setting name.
-   `value` ([Chaîne](../sql-reference/data-types/string.md)) — Setting value.
-   `changed` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
-   `description` ([Chaîne](../sql-reference/data-types/string.md)) — Short setting description.
-   `min` ([Nullable](../sql-reference/data-types/nullable.md)([Chaîne](../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [contraintes](settings/constraints-on-settings.md#constraints-on-settings). Si le réglage n'a pas de valeur minimale, contient [NULL](../sql-reference/syntax.md#null-literal).
-   `max` ([Nullable](../sql-reference/data-types/nullable.md)([Chaîne](../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [contraintes](settings/constraints-on-settings.md#constraints-on-settings). Si le réglage n'a pas de valeur maximale, contient [NULL](../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    -   `0` — Current user can change the setting.
    -   `1` — Current user can't change the setting.

**Exemple**

L'exemple suivant montre comment obtenir des informations sur les paramètres dont le nom contient `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_i%'
```

``` text
┌─name────────────────────────────────────────┬─value─────┬─changed─┬─description───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─min──┬─max──┬─readonly─┐
│ min_insert_block_size_rows                  │ 1048576   │       0 │ Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.                                                                         │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ min_insert_block_size_bytes                 │ 268435456 │       0 │ Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.                                                                        │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ read_backoff_min_interval_between_events_ms │ 1000      │       0 │ Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time. │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
└─────────────────────────────────────────────┴───────────┴─────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────┴──────┴──────────┘
```

À l'aide de `WHERE changed` peut être utile, par exemple, lorsque vous voulez vérifier:

-   Indique si les paramètres des fichiers de configuration sont chargés correctement et sont utilisés.
-   Paramètres modifiés dans la session en cours.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**Voir aussi**

-   [Paramètre](settings/index.md#session-settings-intro)
-   [Autorisations pour les requêtes](settings/permissions-for-queries.md#settings_readonly)
-   [Contraintes sur les paramètres](settings/constraints-on-settings.md)

## système.tableau\_moteurs {#system.table_engines}

``` text
┌─name───────────────────┬─value───────┐
│ max_threads            │ 8           │
│ use_uncompressed_cache │ 0           │
│ load_balancing         │ random      │
│ max_memory_usage       │ 10000000000 │
└────────────────────────┴─────────────┘
```

## système.merge\_tree\_settings {#system-merge_tree_settings}

Contient des informations sur les paramètres pour `MergeTree` table.

Colonne:

-   `name` (String) — Setting name.
-   `value` (String) — Setting value.
-   `description` (String) — Setting description.
-   `type` (String) — Setting type (implementation specific string value).
-   `changed` (UInt8) — Whether the setting was explicitly defined in the config or explicitly changed.

## système.tableau\_moteurs {#system-table-engines}

Contient une description des moteurs de table pris en charge par le serveur et leurs informations de support de fonctionnalité.

Ce tableau contient les colonnes suivantes (le type de colonne est indiqué entre parenthèses):

-   `name` (String) — The name of table engine.
-   `supports_settings` (UInt8) — Flag that indicates if table engine supports `SETTINGS` clause.
-   `supports_skipping_indices` (UInt8) — Flag that indicates if table engine supports [sauter les indices](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — Flag that indicates if table engine supports [TTL](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — Flag that indicates if table engine supports clauses `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` et `SAMPLE_BY`.
-   `supports_replication` (UInt8) — Flag that indicates if table engine supports [réplication des données](../engines/table-engines/mergetree-family/replication.md).
-   `supports_duduplication` (UInt8) — Flag that indicates if table engine supports data deduplication.

Exemple:

``` sql
SELECT *
FROM system.table_engines
WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

``` text
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┐
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┘
```

**Voir aussi**

-   Famille MergeTree [les clauses de requête](../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
-   Kafka [paramètre](../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table)
-   Rejoindre [paramètre](../engines/table-engines/special/join.md#join-limitations-and-settings)

## système.table {#system-tables}

Contient les métadonnées de chaque table que le serveur connaît. Les tableaux détachés ne sont pas représentés dans `system.tables`.

Ce tableau contient les colonnes suivantes (le type de colonne est indiqué entre parenthèses):

-   `database` (String) — The name of the database the table is in.

-   `name` (String) — Table name.

-   `engine` (String) — Table engine name (without parameters).

-   `is_temporary` (UInt8) - indicateur qui indique si la table est temporaire.

-   `data_path` (Chaîne) - chemin d'accès aux données de la table dans le système de fichiers.

-   `metadata_path` (Chaîne) - chemin d'accès aux métadonnées de la table dans le système de fichiers.

-   `metadata_modification_time` (DateTime) - Heure de la dernière modification des métadonnées de la table.

-   `dependencies_database` (Array (String)) - dépendances de base de données.

-   `dependencies_table` (Array (String)) - dépendances de Table ([MaterializedView](../engines/table-engines/special/materializedview.md) tables basées sur le tableau actuel).

-   `create_table_query` (Chaîne) - la requête qui a été utilisée pour créer la table.

-   `engine_full` (Chaîne) - paramètres du moteur de table.

-   `partition_key` (String) - l'expression de clé de partition spécifiée dans le tableau.

-   `sorting_key` (String) - l'expression de clé de tri spécifiée dans la table.

-   `primary_key` (String) - l'expression de clé primaire spécifiée dans la table.

-   `sampling_key` (String) - l'expression de clé d'échantillonnage spécifiée dans la table.

-   `storage_policy` (String) - La politique de stockage:

    -   [MergeTree](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [Distribué](../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable (UInt64)) - nombre Total de lignes, s'il est possible de déterminer rapidement le nombre exact de lignes dans la table, sinon `Null` (y compris underying `Buffer` table).

-   `total_bytes` (Nullable (UInt64)) - nombre Total d'octets, s'il est possible de déterminer rapidement le nombre exact d'octets pour la table sur le stockage, sinon `Null` (**ne pas** comprend tout de stockage sous-jacent).

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   Si la table stocke des données en mémoire, renvoie un nombre approximatif d'octets utilisés en mémoire.

Le `system.tables` le tableau est utilisé dans `SHOW TABLES` implémentation de requête.

## système.zookeeper {#system-zookeeper}

La table n'existe pas si ZooKeeper n'est pas configuré. Permet de lire les données du cluster Zookeeper défini dans la configuration.
La requête doit avoir un ‘path’ condition d'égalité dans la clause WHERE. C'est le chemin dans ZooKeeper pour les enfants pour lesquels vous souhaitez obtenir des données.

Requête `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` données de sortie pour tous les enfants `/clickhouse` nœud.
Pour générer des données pour tous les nœuds racine, écrivez path = ‘/’.
Si le chemin d'accès spécifié dans ‘path’ n'existe pas, une exception sera levée.

Colonne:

-   `name` (String) — The name of the node.
-   `path` (String) — The path to the node.
-   `value` (String) — Node value.
-   `dataLength` (Int32) — Size of the value.
-   `numChildren` (Int32) — Number of descendants.
-   `czxid` (Int64) — ID of the transaction that created the node.
-   `mzxid` (Int64) — ID of the transaction that last changed the node.
-   `pzxid` (Int64) — ID of the transaction that last deleted or added descendants.
-   `ctime` (DateTime) — Time of node creation.
-   `mtime` (DateTime) — Time of the last modification of the node.
-   `version` (Int32) — Node version: the number of times the node was changed.
-   `cversion` (Int32) — Number of added or removed descendants.
-   `aversion` (Int32) — Number of changes to the ACL.
-   `ephemeralOwner` (Int64) — For ephemeral nodes, the ID of the session that owns this node.

Exemple:

``` sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

``` text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```

## système.mutation {#system_tables-mutations}

Le tableau contient des informations sur [mutation](../sql-reference/statements/alter.md#alter-mutations) des tables MergeTree et leur progression. Chaque commande de mutation est représentée par une seule ligne. Le tableau comporte les colonnes suivantes:

**base de données**, **table** - Le nom de la base de données et de la table à laquelle la mutation a été appliquée.

**mutation\_id** - Le numéro d'identification de la mutation. Pour les tables répliquées ces ID correspondent aux noms znode dans le `<table_path_in_zookeeper>/mutations/` répertoire de la Gardienne. Pour les tables non compliquées, Les Id correspondent aux noms de fichiers dans le répertoire de données de la table.

**commande** - La chaîne de commande mutation (la partie de la requête après `ALTER TABLE [db.]table`).

**create\_time** - Quand cette commande de mutation a été soumise pour exécution.

**block\_numbers.partition\_id**, **block\_numbers.nombre** - Une colonne imbriquée. Pour les mutations de tables répliquées, il contient un enregistrement pour chaque partition: l'ID de partition et le numéro de bloc acquis par la mutation (dans chaque partition, seules les parties contenant des blocs avec des nombres inférieurs au numéro de bloc acquis par la mutation dans cette partition seront mutées). Dans les tables non répliquées, les numéros de bloc de toutes les partitions forment une seule séquence. Cela signifie que pour les mutations de tables non répliquées, la colonne contiendra un enregistrement avec un seul numéro de bloc acquis par la mutation.

**parts\_to\_do** - Le nombre de parties de données qui doivent être mutées pour que la mutation se termine.

**\_done** - La mutation est faite? Notez que même si `parts_to_do = 0` il est possible qu'une mutation d'une table répliquée ne soit pas encore effectuée en raison d'un INSERT de longue durée qui créera une nouvelle partie de données qui devra être mutée.

S'il y avait des problèmes avec la mutation de certaines parties, les colonnes suivantes contiennent des informations supplémentaires:

**latest\_failed\_part** - Le nom de la partie la plus récente qui n'a pas pu être mutée.

**latest\_fail\_time** - Le temps de la partie la plus récente mutation de l'échec.

**latest\_fail\_reason** - Le message d'exception qui a provoqué l'échec de la mutation de pièce la plus récente.

## système.disque {#system_tables-disks}

Contient des informations sur les disques définis dans [configuration du serveur](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Colonne:

-   `name` ([Chaîne](../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
-   `path` ([Chaîne](../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
-   `free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
-   `total_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
-   `keep_free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` paramètre de configuration du disque.

## système.storage\_policies {#system_tables-storage_policies}

Contient des informations sur les stratégies de stockage et les volumes définis [configuration du serveur](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Colonne:

-   `policy_name` ([Chaîne](../sql-reference/data-types/string.md)) — Name of the storage policy.
-   `volume_name` ([Chaîne](../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
-   `volume_priority` ([UInt64](../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration.
-   `disks` ([Tableau(String)](../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
-   `max_data_part_size` ([UInt64](../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
-   `move_factor` ([Float64](../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.

Si la stratégie de stockage contient plus d'un volume, les informations pour chaque volume sont stockées dans la ligne individuelle de la table.

[Article Original](https://clickhouse.tech/docs/en/operations/system_tables/) <!--hide-->
