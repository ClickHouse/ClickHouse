---
description: 'Documentation des instructions SYSTEM'
sidebar_label: 'SYSTEM'
sidebar_position: 36
slug: /sql-reference/statements/system
title: 'Instructions SYSTEM'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="system-statements">
  # Instructions SYSTEM
</div>

<div id="reload-embedded-dictionaries">
  ## SYSTEM RELOAD EMBEDDED DICTIONARIES
</div>

Recharge l’ensemble des [dictionnaires internes](./create/dictionary/overview.md).
Par défaut, les dictionnaires internes sont désactivés.
Renvoyer toujours `Ok.` quel que soit le résultat de la mise à jour des dictionnaires internes.

<div id="reload-dictionaries">
  ## SYSTEM RELOAD DICTIONARIES
</div>

La requête `SYSTEM RELOAD DICTIONARIES` recharge les dictionnaires dont le statut est `LOADED` (voir la colonne `status` de [`system.dictionaries`](/fr/operations/system-tables/dictionaries)), c’est-à-dire les dictionnaires qui ont déjà été chargés avec succès.
Par défaut, les dictionnaires sont chargés à la demande (voir [dictionaries&#95;lazy&#95;load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load)) ; ainsi, au lieu d’être chargés automatiquement au démarrage, ils sont initialisés lors du premier accès, via la fonction [`dictGet`](/fr/sql-reference/functions/ext-dict-functions#dictGet) ou par une requête `SELECT` sur des tables avec `ENGINE = Dictionary`.

**Syntaxe**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

<div id="reload-dictionary">
  ## SYSTEM RELOAD DICTIONARY
</div>

Recharge intégralement un dictionnaire `dictionary_name`, quel que soit son état (LOADED / NOT&#95;LOADED / FAILED).
Renvoie toujours `Ok.`, quel que soit le résultat de la mise à jour du dictionnaire.

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

L’état du dictionnaire peut être vérifié en interrogeant la table `system.dictionaries`.

```sql
SELECT name, status FROM system.dictionaries;
```

<div id="reload-models">
  ## SYSTEM RELOAD MODELS
</div>

:::note
Cette instruction et `SYSTEM RELOAD MODEL` se contentent de décharger les modèles CatBoost de clickhouse-library-bridge. La fonction `catboostEvaluate()`
charge un modèle lors du premier accès s’il n’est pas encore chargé.
:::

Décharge tous les modèles CatBoost.

**Syntaxe**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

<div id="reload-model">
  ## SYSTEM RELOAD MODEL
</div>

Décharge le modèle CatBoost situé à `model_path`.

**Syntaxe**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

<div id="reload-functions">
  ## SYSTEM RELOAD FUNCTIONS
</div>

Recharge toutes les [fonctions définies par l’utilisateur exécutables](/fr/sql-reference/functions/udf#executable-user-defined-functions) enregistrées, ou l’une d’entre elles, à partir d’un fichier de configuration.

**Syntaxe**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

<div id="reload-asynchronous-metrics">
  ## SYSTEM RELOAD ASYNCHRONOUS METRICS
</div>

Recalcule toutes les [métriques asynchrones](../../operations/system-tables/asynchronous_metrics.md). Comme les métriques asynchrones sont mises à jour périodiquement selon le paramètre [asynchronous&#95;metrics&#95;update&#95;period&#95;s](../../operations/server-configuration-parameters/settings.md), il n’est généralement pas nécessaire de les actualiser manuellement à l’aide de cette instruction.

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

<div id="drop-dns-cache">
  ## SYSTEM CLEAR|DROP DNS CACHE
</div>

Vide le cache DNS interne de ClickHouse. Il est parfois nécessaire (pour les anciennes versions de ClickHouse) d&#39;utiliser cette commande lors d&#39;une modification de l&#39;infrastructure (par exemple, lors du changement d&#39;adresse IP d&#39;un autre serveur ClickHouse ou du serveur utilisé par les dictionnaires).

Pour une gestion plus pratique (automatique) du cache, consultez les paramètres `disable_internal_dns_cache`, `dns_cache_max_entries`, `dns_cache_update_period`.

<div id="drop-mark-cache">
  ## SYSTEM CLEAR|DROP MARK CACHE
</div>

Efface le cache des marks.

<div id="drop-primary-index-cache">
  ## SYSTEM CLEAR|DROP PRIMARY INDEX CACHE
</div>

Vide le cache de l’index primaire, qui stocke en mémoire les clés primaires des tables [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md).
Sa taille est définie par le paramètre de niveau serveur [`primary_index_cache_size`](../../operations/server-configuration-parameters/settings.md#primary_index_cache_size).

<div id="drop-iceberg-metadata-cache">
  ## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE
</div>

Vide le cache de métadonnées Iceberg.

<div id="drop-avro-schema-cache">
  ## SYSTEM CLEAR|DROP AVRO SCHEMA CACHE
</div>

Efface les caches par URL du Confluent Schema Registry utilisés par le format `AvroConfluent`. Cela supprime à la fois le cache de récupération des schémas (id → schéma) et le cache d’enregistrement des schémas (subject + schéma → id), de sorte que les lectures et écritures suivantes repassent par le serveur de registre. Utile lorsqu’un schéma a été supprimé ou réécrit côté registre, ou pour vérifier l’idempotence du registre dans les tests.

<div id="drop-parquet-metadata-cache">
  ## SYSTEM DROP PARQUET METADATA CACHE
</div>

Vide le cache des métadonnées Parquet.

<div id="drop-point-in-polygon-cache">
  ## SYSTEM CLEAR|DROP POINT IN POLYGON CACHE
</div>

Efface le cache des polygones constants prétraités utilisés par la fonction [`pointInPolygon`](../functions/geo/coordinates.md#pointinpolygon). La limite de taille configurée (le paramètre du serveur `point_in_polygon_cache_size`) reste inchangée, de sorte que le cache continue ensuite à accepter de nouvelles entrées. Pour désactiver le cache, définissez plutôt `point_in_polygon_cache_size` sur `0`.

<div id="drop-text-index-caches">
  ## SYSTEM CLEAR|DROP TEXT INDEX CACHES
</div>

Vide les caches des jetons, de l’en-tête et des postings de l’index de texte.

Si vous souhaitez supprimer l’un de ces caches individuellement, vous pouvez exécuter

* `SYSTEM CLEAR TEXT INDEX TOKENS CACHE`,
* `SYSTEM CLEAR TEXT INDEX HEADER CACHE`, ou
* `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

<div id="drop-index-mark-cache">
  ## SYSTEM CLEAR|DROP INDEX MARK CACHE
</div>

Vide le cache des marks des index secondaires (data-skipping).

<div id="drop-index-uncompressed-cache">
  ## SYSTEM CLEAR|DROP INDEX UNCOMPRESSED CACHE
</div>

Vide le cache des blocs non compressés des index secondaires (data-skipping).

<div id="drop-mmap-cache">
  ## SYSTEM CLEAR|DROP MMAP CACHE
</div>

Vide le cache des fichiers mappés en mémoire.

<div id="drop-page-cache">
  ## SYSTEM CLEAR|DROP PAGE CACHE
</div>

Vide le cache de pages en espace utilisateur, c’est-à-dire le propre cache en mémoire de ClickHouse pour les données lues depuis le stockage sous-jacent.

<div id="drop-vector-similarity-index-cache">
  ## SYSTEM CLEAR|DROP VECTOR SIMILARITY INDEX CACHE
</div>

Vide le cache de l’index de similarité vectorielle.

<div id="drop-connections-cache">
  ## SYSTEM CLEAR|DROP CONNECTIONS CACHE
</div>

Vide le cache des pools de connexions HTTP utilisés pour les connexions sortantes.

<div id="drop-s3-client-cache">
  ## SYSTEM CLEAR|DROP S3 CLIENT CACHE
</div>

Vide le cache des clients S3.

<div id="prewarm-mark-cache">
  ## SYSTEM PREWARM MARK CACHE
</div>

Charge les marks d’une table dans le [cache des marks](#drop-mark-cache). Les marks des index secondaires sont également chargés dans le [cache des index marks](#drop-index-mark-cache).

```sql
SYSTEM PREWARM MARK CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="prewarm-primary-index-cache">
  ## SYSTEM PREWARM PRIMARY INDEX CACHE
</div>

Charge les index primaires d’une table `MergeTree` dans le [cache de l’index primaire](#drop-primary-index-cache).

```sql
SYSTEM PREWARM PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="drop-disk-metadata-cache">
  ## SYSTEM CLEAR|DROP DISK METADATA CACHE
</div>

Efface le cache de métadonnées du disque spécifié.

```sql
SYSTEM DROP DISK METADATA CACHE <disk_name>
```

<div id="sync-filesystem-cache">
  ## SYSTEM SYNC FILESYSTEM CACHE
</div>

Synchronise l’état en mémoire du cache du système de fichiers de ClickHouse avec les fichiers de cache effectivement présents sur le disque, et renvoie, pour chaque segment de fichier mis en cache, les valeurs de `cache_name`, `path` et `size` téléchargée. Un nom de cache facultatif permet de limiter l’opération à un seul cache.

```sql
SYSTEM SYNC FILESYSTEM CACHE ['<cache_name>']
```

<div id="drop-distributed-cache">
  ## SYSTEM CLEAR|DROP DISTRIBUTED CACHE
</div>

:::note
`SYSTEM CLEAR|DROP DISTRIBUTED CACHE` est disponible uniquement dans ClickHouse Cloud.
:::

Supprime le cache distribué. Utilisez `CONNECTIONS` pour supprimer uniquement les connexions en cache vers les serveurs du cache distribué, ou indiquez un identifiant de serveur pour cibler un seul serveur.

```sql
SYSTEM DROP DISTRIBUTED CACHE [CONNECTIONS | 'server_id']
```

<div id="drop-replica">
  ## SYSTEM DROP REPLICA
</div>

Les répliques défaillantes des tables `ReplicatedMergeTree` peuvent être supprimées à l’aide de la syntaxe suivante :

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

Les requêtes suppriment le chemin de la réplique `ReplicatedMergeTree` dans ZooKeeper. Cela est utile lorsque la réplique est hors service et que ses métadonnées ne peuvent pas être supprimées de ZooKeeper avec `DROP TABLE`, parce que la table n&#39;existe plus. Elles ne suppriment que la réplique inactive/obsolète et ne peuvent pas supprimer la réplique locale ; veuillez utiliser `DROP TABLE` pour cela. `DROP REPLICA` ne supprime aucune table et ne retire ni données ni métadonnées du disque.

La première supprime les métadonnées de la réplique `'replica_name'` de la table `database.table`.
La deuxième fait de même pour toutes les tables répliquées de la base de données.
La troisième fait de même pour toutes les tables répliquées sur le serveur local.
La quatrième est utile pour supprimer les métadonnées d&#39;une réplique hors service lorsque toutes les autres répliques d&#39;une table ont été supprimées. Elle exige que le chemin de la table soit indiqué explicitement. Il doit s&#39;agir du même chemin que celui passé en premier argument au moteur `ReplicatedMergeTree` lors de la création de la table.

<div id="drop-database-replica">
  ## SYSTEM DROP DATABASE REPLICA
</div>

Les répliques défaillantes des bases de données `Replicated` peuvent être supprimées avec la syntaxe suivante :

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

Semblable à `SYSTEM DROP REPLICA`, mais supprime le chemin de la réplique de la base de données `Replicated` dans ZooKeeper lorsqu’il n’y a pas de base de données sur laquelle exécuter `DROP DATABASE`. Notez que cela ne supprime pas les répliques `ReplicatedMergeTree` (il se peut donc que vous ayez également besoin de `SYSTEM DROP REPLICA`). Les noms de shard et de réplique sont ceux spécifiés dans les arguments du moteur `Replicated` lors de la création de la base de données. Ces noms peuvent aussi être obtenus à partir des colonnes `database_shard_name` et `database_replica_name` de `system.clusters`. Si la clause `FROM SHARD` est absente, `replica_name` doit alors être un nom de réplique complet au format `shard_name|replica_name`.

<div id="drop-uncompressed-cache">
  ## SYSTEM CLEAR|DROP UNCOMPRESSED CACHE
</div>

Vide le cache des données non compressées.
Le cache des données non compressées est activé ou désactivé à l’aide du paramètre au niveau de la requête, de l’utilisateur ou du profil [`use_uncompressed_cache`](../../operations/settings/settings.md#use_uncompressed_cache).
Sa taille peut être configurée à l’aide du paramètre au niveau du serveur [`uncompressed_cache_size`](../../operations/server-configuration-parameters/settings.md#uncompressed_cache_size).

<div id="drop-compiled-expression-cache">
  ## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE
</div>

Efface le cache des expressions compilées.
Le cache des expressions compilées est activé/désactivé avec le paramètre au niveau de la requête, de l’utilisateur ou du profil [`compile_expressions`](../../operations/settings/settings.md#compile_expressions).

<div id="drop-query-condition-cache">
  ## SYSTEM CLEAR|DROP QUERY CONDITION CACHE
</div>

Vide le cache des conditions de requête.

<div id="drop-query-cache">
  ## SYSTEM CLEAR|DROP QUERY CACHE
</div>

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
```

Vide le [cache de requêtes](../../operations/query-cache.md).
Si un tag est spécifié, seules les entrées du cache de requêtes portant ce tag sont supprimées.

<div id="system-drop-schema-format">
  ## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE
</div>

Vide le cache des schémas chargés depuis [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path).

Cibles prises en charge :

* Protobuf : Supprime de la mémoire les définitions de messages Protobuf importées.
* Files : Supprime les fichiers de schéma mis en cache stockés localement dans [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path), générés lorsque `format_schema_source` est défini sur `query`.
  Remarque : si aucune cible n&#39;est spécifiée, les deux caches sont vidés.

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

<div id="flush-logs">
  ## SYSTEM FLUSH LOGS
</div>

Force l’écriture des messages de log mis en mémoire tampon dans les tables système, par exemple `system.query&#95;log`. Cette commande est surtout utile pour le débogage, car la plupart des tables système ont un intervalle de vidage par défaut de 7,5 secondes.
Elle crée également les tables système même si la file d’attente des messages est vide.

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

Si vous ne voulez pas tout vider, vous pouvez forcer l’écriture d’un ou de plusieurs logs spécifiques en indiquant soit leur nom, soit leur table cible :

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

<div id="reload-config">
  ## SYSTEM RELOAD CONFIG
</div>

Recharge la configuration de ClickHouse. S&#39;utilise lorsque la configuration est stockée dans ZooKeeper. Notez que `SYSTEM RELOAD CONFIG` ne recharge pas la configuration `USER` stockée dans ZooKeeper ; il recharge uniquement la configuration `USER` stockée dans `users.xml`. Pour recharger toute la configuration `USER`, utilisez `SYSTEM RELOAD USERS`

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

<div id="reload-users">
  ## SYSTEM RELOAD USERS
</div>

Recharge tous les stockages d’accès, y compris : users.xml, le stockage d’accès sur disque local et le stockage d’accès répliqué (dans ZooKeeper).

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

<div id="shutdown">
  ## SYSTEM SHUTDOWN
</div>

<CloudNotSupportedBadge />

Arrête ClickHouse de manière normale (comme `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

<div id="kill">
  ## SYSTEM KILL
</div>

Met fin au processus ClickHouse (comme `kill -9 {$ pid_clickhouse-server}`)

<div id="instrument">
  ## SYSTEM INSTRUMENT
</div>

Gère les points d&#39;instrumentation à l&#39;aide de la fonctionnalité XRay de LLVM, disponible lorsque ClickHouse est compilé avec `ENABLE_XRAY=1`.
Cela permet de faire du débogage et du profilage en production sans modifier le code source, avec une surcharge minimale.
Lorsqu&#39;aucun point d&#39;instrumentation n&#39;est ajouté, l&#39;impact sur les performances est négligeable, car cela ajoute uniquement un saut supplémentaire vers une adresse proche
au prologue et à l&#39;épilogue des fonctions de plus de 200 instructions.

<div id="instrument-add">
  ### SYSTEM INSTRUMENT ADD
</div>

Ajoute un nouveau point d’instrumentation. Les fonctions instrumentées peuvent être consultées dans la table système [`system.instrumentation`](../../operations/system-tables/instrumentation.md). Plusieurs handlers peuvent être ajoutés à une même fonction, et ils seront exécutés dans le même ordre que celui dans lequel l’instrumentation a été ajoutée.
Les fonctions à instrumenter peuvent être récupérées depuis la table système [`system.symbols`](../../operations/system-tables/symbols.md).

Il existe trois types différents de handlers à ajouter aux fonctions :

**Syntax**

```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [ARGUMENTS]
```

où `FUNCTION` désigne n’importe quelle fonction ou sous-chaîne d’une fonction, telle que `QueryMetricLog::startQuery`, et où le gestionnaire est l’un des suivants

<div id="instrument-add-log">
  #### LOG
</div>

Affiche le texte fourni en argument ainsi que la stack trace lors de l&#39;`ENTRY` ou de l&#39;`EXIT` de la fonction.

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

<div id="instrument-add-sleep">
  #### SLEEP
</div>

Effectue une attente pendant un nombre fixe de secondes à l’étape `ENTRY` ou `EXIT` :

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

ou pour un nombre de secondes aléatoire suivant une distribution uniforme, en indiquant un minimum et un maximum séparés par un espace :

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

<div id="instrument-add-profile">
  #### PROFILE
</div>

Mesure le temps écoulé entre `ENTRY` et `EXIT` d&#39;une fonction.
Le résultat du profiling est stocké dans [`system.trace_log`](../../operations/system-tables/trace_log.md) et peut être converti
au [format Chrome Event Trace](../../operations/system-tables/trace_log.md#chrome-event-trace-format).

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

<div id="instrument-remove">
  ### SYSTEM INSTRUMENT REMOVE
</div>

Supprime soit un seul point d’instrumentation à l’aide de :

```sql
SYSTEM INSTRUMENT REMOVE ID
```

toutes avec le mot-clé `ALL` :

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

un ensemble d’identifiants issu d’une sous-requête :

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

ou tous les points d’instrumentation qui correspondent à un function&#95;name donné :

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

Les informations sur les points d’instrumentation peuvent être obtenues à partir de la table système [`system.instrumentation`](../../operations/system-tables/instrumentation.md).

<div id="managing-distributed-tables">
  ## Gestion des tables Distributed
</div>

ClickHouse permet de gérer les tables [Distributed](../../engines/table-engines/special/distributed.md). Lorsqu&#39;un utilisateur insère des données dans ces tables, ClickHouse crée d&#39;abord une file d&#39;attente contenant les données à envoyer aux nœuds du cluster, puis les envoie de manière asynchrone. Vous pouvez gérer le traitement de cette file d&#39;attente avec les requêtes [`STOP DISTRIBUTED SENDS`](#stop-distributed-sends), [FLUSH DISTRIBUTED](#flush-distributed) et [`START DISTRIBUTED SENDS`](#start-distributed-sends). Vous pouvez également insérer des données distribuées de manière synchrone à l&#39;aide du paramètre [`distributed_foreground_insert`](../../operations/settings/settings.md#distributed_foreground_insert).

<div id="stop-distributed-sends">
  ### SYSTEM STOP DISTRIBUTED SENDS
</div>

Désactive la distribution des données en arrière-plan lors de l’insertion de données dans des tables distribuées.

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

:::note
Si [`prefer_localhost_replica`](../../operations/settings/settings.md#prefer_localhost_replica) est activé (par défaut), les données seront de toute façon insérées sur le shard local.
:::

<div id="flush-distributed">
  ### SYSTEM FLUSH DISTRIBUTED
</div>

Force ClickHouse à envoyer les données aux nœuds du cluster de façon synchrone. Si certains nœuds sont indisponibles, ClickHouse lève une exception et arrête l’exécution de la requête. Vous pouvez réessayer la requête jusqu’à ce qu’elle aboutisse, ce qui se produira lorsque tous les nœuds seront de nouveau en ligne.

Vous pouvez également remplacer certains paramètres via la clause `SETTINGS` ; cela peut être utile pour contourner certaines limitations temporaires, comme `max_concurrent_queries_for_all_users` ou `max_memory_usage`.

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
Chaque bloc en attente est stocké sur disque avec les paramètres de la requête INSERT initiale, c’est pourquoi vous pouvez parfois vouloir modifier ces paramètres.
:::

<div id="start-distributed-sends">
  ### SYSTEM START DISTRIBUTED SENDS
</div>

Active la distribution en arrière-plan des données lors de leur insertion dans des tables distribuées.

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

<div id="stop-listen">
  ### SYSTEM STOP LISTEN
</div>

Ferme le socket et met fin proprement aux connexions existantes au serveur sur le port spécifié avec le protocole spécifié.

Toutefois, si les paramètres du protocole correspondant n’ont pas été spécifiés dans la configuration de clickhouse-server, cette commande sera sans effet.

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

* Si le modificateur `CUSTOM 'protocol'` est spécifié, le protocole personnalisé du nom indiqué, défini dans la section des protocoles de la configuration du serveur, sera arrêté.
* Si le modificateur `QUERIES ALL [EXCEPT .. [,..]]` est spécifié, tous les protocoles sont arrêtés, sauf ceux indiqués dans la clause `EXCEPT`.
* Si le modificateur `QUERIES DEFAULT [EXCEPT .. [,..]]` est spécifié, tous les protocoles par défaut sont arrêtés, sauf ceux indiqués dans la clause `EXCEPT`.
* Si le modificateur `QUERIES CUSTOM [EXCEPT .. [,..]]` est spécifié, tous les protocoles personnalisés sont arrêtés, sauf ceux indiqués dans la clause `EXCEPT`.

<div id="start-listen">
  ### SYSTEM START LISTEN
</div>

Permet d’établir de nouvelles connexions via les protocoles spécifiés.

Cependant, si le serveur sur le port et le protocole spécifiés n’a pas été arrêté à l’aide de la commande SYSTEM STOP LISTEN, cette commande sera sans effet.

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

<div id="managing-mergetree-tables">
  ## Gestion des tables MergeTree
</div>

ClickHouse peut gérer les processus d’arrière-plan dans les tables [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

<div id="stop-merges">
  ### SYSTEM STOP MERGES
</div>

<CloudNotSupportedBadge />

Permet d’arrêter les fusions en arrière-plan des tables de la famille MergeTree :

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
`DETACH / ATTACH` d’une table relance les merges en arrière-plan de la table, même si les merges ont auparavant été arrêtés pour toutes les tables MergeTree.
:::

<div id="start-merges">
  ### SYSTEM START MERGES
</div>

<CloudNotSupportedBadge />

Permet de démarrer les fusions d’arrière-plan pour les tables de la famille MergeTree :

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

<div id="stop-ttl-merges">
  ### SYSTEM STOP TTL MERGES
</div>

<CloudNotSupportedBadge />

Permet d’arrêter la suppression en arrière-plan des anciennes données selon l’[expression TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) pour les tables de la famille MergeTree :
Renvoie `Ok.` même si la table n’existe pas ou si elle n’utilise pas le moteur MergeTree. Renvoie une erreur lorsque la base de données n’existe pas :

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-ttl-merges">
  ### SYSTEM START TTL MERGES
</div>

<CloudNotSupportedBadge />

Permet de démarrer en arrière-plan la suppression des anciennes données conformément à l’[expression TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) pour les tables de la famille MergeTree :
Retourne `Ok.` même si la table n’existe pas. Retourne une erreur si la base de données n’existe pas :

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="stop-moves">
  ### SYSTEM STOP MOVES
</div>

Permet d&#39;arrêter les déplacements de données en arrière-plan selon l&#39;[expression TTL de table avec la clause TO VOLUME ou TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) pour les tables de la famille MergeTree :
Renvoie `Ok.` même si la table n&#39;existe pas. Renvoie une erreur lorsque la base de données n&#39;existe pas :

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-moves">
  ### SYSTEM START MOVES
</div>

Permet de lancer les déplacements de données en arrière-plan conformément à l’[expression TTL de table avec les clauses TO VOLUME et TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) pour les tables de la famille MergeTree :
Renvoie `Ok.` même si la table n’existe pas. Renvoie une erreur lorsque la base de données n’existe pas :

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="query_language-system-unfreeze">
  ### SYSTEM UNFREEZE
</div>

Supprime de tous les disques une sauvegarde gelée portant le nom spécifié. Pour en savoir plus sur le dégel de parties individuelles, consultez [ALTER TABLE table&#95;name UNFREEZE WITH NAME ](/fr/sql-reference/statements/alter/partition#unfreeze-partition)

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

<div id="wait-loading-parts">
  ### SYSTEM WAIT LOADING PARTS
</div>

Attendez que toutes les parties de données d’une table chargées de manière asynchrone (parties de données obsolètes) soient entièrement chargées.

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

<div id="managing-replicatedmergetree-tables">
  ## Gestion des tables ReplicatedMergeTree
</div>

ClickHouse peut gérer les processus en arrière-plan liés à la réplication dans les tables [ReplicatedMergeTree](/fr/engines/table-engines/mergetree-family/replication).

<div id="stop-fetches">
  ### SYSTEM STOP FETCHES
</div>

<CloudNotSupportedBadge />

Permet d’arrêter les récupérations en tâche de fond des parts insérées pour les tables de la famille `ReplicatedMergeTree` :
Renvoie toujours `Ok.`, quel que soit le moteur de la table, même si la table ou la base de données n’existe pas.

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-fetches">
  ### SYSTEM START FETCHES
</div>

<CloudNotSupportedBadge />

Permet de démarrer les opérations de récupération en arrière-plan des parts insérées pour les tables de la famille `ReplicatedMergeTree` :
Renvoie toujours `Ok.`, quel que soit le moteur de table, même si la table ou la base de données n’existe pas.

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replicated-sends">
  ### SYSTEM STOP REPLICATED SENDS
</div>

Permet d’arrêter l’envoi en arrière-plan vers les autres répliques du cluster des nouvelles parts insérées dans les tables de la famille `ReplicatedMergeTree` :

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replicated-sends">
  ### SYSTEM START REPLICATED SENDS
</div>

Permet de démarrer en arrière-plan l’envoi vers d’autres répliques du cluster des nouvelles parts insérées pour les tables de la famille `ReplicatedMergeTree` :

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replication-queues">
  ### SYSTEM STOP REPLICATION QUEUES
</div>

Permet d&#39;arrêter les tâches de fetch en arrière-plan des files d&#39;attente de réplication stockées dans ZooKeeper pour les tables de la famille `ReplicatedMergeTree`. Types de tâches en arrière-plan possibles : merges, fetches, mutation, instructions DDL avec la clause ON CLUSTER :

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replication-queues">
  ### SYSTEM START REPLICATION QUEUES
</div>

Permet de démarrer des tâches de récupération en arrière-plan à partir des files d’attente de réplication stockées dans ZooKeeper pour les tables de la famille `ReplicatedMergeTree`. Types possibles de tâches d’arrière-plan - merges, fetches, mutation, instructions DDL avec la clause ON CLUSTER :

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-pulling-replication-log">
  ### SYSTEM STOP PULLING REPLICATION LOG
</div>

Arrête le chargement de nouvelles entrées depuis le journal de réplication vers la file d&#39;attente de réplication d&#39;une table `ReplicatedMergeTree`.

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-pulling-replication-log">
  ### SYSTEM START PULLING REPLICATION LOG
</div>

Annule `SYSTEM STOP PULLING REPLICATION LOG`.

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="sync-replica">
  ### SYSTEM SYNC REPLICA
</div>

Attendez qu’une table `ReplicatedMergeTree` soit synchronisée avec les autres répliques d’un cluster, dans un délai maximal de `receive_timeout` secondes.

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

Après l’exécution de cette instruction, `[db.]replicated_merge_tree_family_table_name` récupère les commandes du journal répliqué commun dans sa propre file d’attente de réplication, puis la requête attend que la réplique les traite toutes. Les modificateurs suivants sont pris en charge :

* Avec `IF EXISTS` (disponible depuis la version 25.6), la requête ne renverra pas d’erreur si la table n’existe pas. Cela est utile lors de l’ajout d’une nouvelle réplique à un cluster, lorsqu’elle fait déjà partie de la configuration du cluster mais qu’elle est encore en cours de création et de synchronisation de la table.
* Si un modificateur `STRICT` est spécifié, la requête attend que la file d’attente de réplication soit vide. La version `STRICT` peut ne jamais aboutir si de nouvelles entrées continuent d’apparaître dans la file d’attente de réplication.
* Si un modificateur `LIGHTWEIGHT` est spécifié, la requête attend uniquement le traitement des entrées `GET_PART`, `ATTACH_PART`, `DROP_RANGE`, `REPLACE_RANGE` et `DROP_PART`.
  De plus, le modificateur LIGHTWEIGHT prend en charge une clause FROM &#39;srcReplicas&#39; facultative, où &#39;srcReplicas&#39; est une liste de noms de répliques source séparés par des virgules. Cette extension permet une synchronisation plus ciblée en se concentrant uniquement sur les tâches de réplication provenant des répliques source spécifiées.
* Si un modificateur `PULL` est spécifié, la requête récupère de nouvelles entrées de la file d’attente de réplication depuis ZooKeeper, mais n’attend le traitement d’aucune d’entre elles.

<div id="sync-database-replica">
  ### SYNC DATABASE REPLICA
</div>

Attend que la [base de données répliquée](/fr/engines/database-engines/replicated) spécifiée ait appliqué toutes les modifications de schéma de la file DDL de cette base de données.

**Syntaxe**

```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

<div id="restart-replica">
  ### SYSTEM RESTART REPLICA
</div>

Permet de réinitialiser l’état de la session ZooKeeper pour la table `ReplicatedMergeTree`, de comparer l’état actuel avec ZooKeeper comme source de référence et, si nécessaire, d’ajouter des tâches à la file d’attente de ZooKeeper.
L’initialisation de la file d’attente de réplication à partir des données ZooKeeper s’effectue de la même manière que pour l’instruction `ATTACH TABLE`. Pendant une courte période, la table sera indisponible pour toute opération.

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

<div id="restore-replica">
  ### SYSTEM RESTORE REPLICA
</div>

Restaure une réplique si les données sont [éventuellement] présentes, mais que les métadonnées ZooKeeper ont été perdues.

Fonctionne uniquement sur les tables `ReplicatedMergeTree` en lecture seule.

La requête peut être exécutée après :

* la perte de la racine ZooKeeper `/`.
* la perte du chemin des répliques `/replicas`.
* la perte du chemin d&#39;une réplique donnée `/replicas/replica_name/`.

La réplique attache les parts trouvées localement et envoie les informations les concernant à ZooKeeper.
Les parts présentes sur une réplique avant la perte des métadonnées ne sont pas récupérées à nouveau depuis d&#39;autres répliques si elles ne sont pas obsolètes (la restauration d&#39;une réplique ne signifie donc pas le retéléchargement de toutes les données via le réseau).

:::note
Les parts, dans tous les états, sont déplacées vers le dossier `detached/`. Les parts actives avant la perte des données (committed) sont attachées.
:::

<div id="restore-database-replica">
  ### SYSTEM RESTORE DATABASE REPLICA
</div>

Restaure une réplique si des données sont [potentiellement] présentes, mais que les métadonnées ZooKeeper ont été perdues.

**Syntaxe**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**Exemple**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

**Syntaxe**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

Syntaxe alternative :

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**Exemple**

Création d’une table sur plusieurs serveurs. Après la perte des métadonnées de la réplique dans ZooKeeper, la table sera attachée en lecture seule, car les métadonnées sont absentes. La dernière requête doit être exécutée sur chaque réplique.

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

Autre méthode :

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

<div id="restart-replicas">
  ### SYSTEM RESTART REPLICAS
</div>

Permet de réinitialiser l’état des sessions ZooKeeper pour toutes les tables `ReplicatedMergeTree`, compare l’état actuel à celui de ZooKeeper comme source de vérité et ajoute des tâches à la file d’attente de ZooKeeper si nécessaire

<div id="drop-filesystem-cache">
  ### SYSTEM CLEAR|DROP FILESYSTEM CACHE
</div>

Permet de vider le cache du système de fichiers.

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

<div id="sync-file-cache">
  ### SYSTEM SYNC FILE CACHE
</div>

:::note
C&#39;est trop lourd et risque d&#39;être mal utilisé.
:::

Effectue l&#39;appel système sync.

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

<div id="load-primary-key">
  ### SYSTEM LOAD PRIMARY KEY
</div>

Charge les clés primaires de la table spécifiée ou de toutes les tables.

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

<div id="unload-primary-key">
  ### SYSTEM UNLOAD PRIMARY KEY
</div>

Décharge les clés primaires de la table spécifiée ou de toutes les tables.

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

<div id="managing-refreshable-materialized-views">
  ## Gestion des vues matérialisées actualisables
</div>

Commandes permettant de contrôler les tâches exécutées en arrière-plan par les [vues matérialisées actualisables](../../sql-reference/statements/create/view.md#refreshable-materialized-view)

Surveillez [`system.view_refreshes`](../../operations/system-tables/view_refreshes.md) lorsque vous les utilisez.

<div id="stop-view-stop-views">
  ### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS
</div>

Désactive l’actualisation périodique de la vue indiquée ou de toutes les vues actualisables. Si une actualisation est en cours, elle est également annulée.

Si la vue se trouve dans une base de données Replicated ou Shared, `STOP VIEW` n’affecte que la réplique actuelle, tandis que `STOP REPLICATED VIEW` affecte toutes les répliques.

:::note
L’état d’arrêt ne persiste pas après un redémarrage du serveur. Après le redémarrage, les vues reprennent leur planification d’actualisation configurée.
Dans les bases de données Replicated ou Shared, `SYSTEM STOP VIEW` n’affecte que la réplique actuelle. Utilisez `SYSTEM STOP REPLICATED VIEW` pour arrêter les actualisations sur toutes les répliques.
:::

```sql
SYSTEM STOP VIEW [db.]name
```

```sql
SYSTEM STOP VIEWS
```

<div id="start-view-start-views">
  ### SYSTEM START [REPLICATED] VIEW, START VIEWS
</div>

Active l’actualisation périodique pour la vue spécifiée ou pour toutes les vues actualisables. Aucun rafraîchissement immédiat n’est déclenché.

Si la vue se trouve dans une base de données Replicated ou Shared, `START VIEW` annule l’effet de `STOP VIEW` et `START REPLICATED VIEW` annule l’effet de `STOP REPLICATED VIEW`. `START VIEW` annule également l’effet de `PAUSE VIEW`.

```sql
SYSTEM START VIEW [db.]name
```

```sql
SYSTEM START VIEWS
```

<div id="pause-view-pause-views">
  ### SYSTEM PAUSE VIEW, PAUSE VIEWS
</div>

Désactive l’actualisation périodique de la vue spécifiée ou de toutes les vues actualisables.
Contrairement à `SYSTEM STOP VIEW`, `SYSTEM PAUSE VIEW` n’interrompt pas une actualisation déjà en cours : l’actualisation en cours est autorisée à se terminer, et seules les actualisations suivantes sont empêchées.

Pour annuler, utilisez `SYSTEM START VIEW` ou `SYSTEM START VIEWS`.

:::note
L’état de pause ne persiste pas après un redémarrage du serveur. Après le redémarrage, les vues reprendront leur cadence d’actualisation configurée.
Dans les bases de données Replicated ou Shared, `SYSTEM PAUSE VIEW` n’affecte que la réplique actuelle.
:::

```sql
SYSTEM PAUSE VIEW [db.]name
```

```sql
SYSTEM PAUSE VIEWS
```

<div id="refresh-view">
  ### SYSTEM REFRESH VIEW
</div>

Déclenche immédiatement l’actualisation d’une vue donnée, en dehors de la planification prévue.

```sql
SYSTEM REFRESH VIEW [db.]name
```

<div id="wait-view">
  ### SYSTEM WAIT VIEW
</div>

Attend que l’actualisation en cours soit terminée. Si aucune actualisation n’est en cours, renvoie immédiatement. Si la dernière tentative d’actualisation a échoué, signale une erreur.

Peut être utilisée juste après la création d’une nouvelle vue matérialisée actualisable (sans le mot-clé EMPTY) pour attendre la fin de l’actualisation initiale.

Si la vue se trouve dans une base de données Replicated ou Shared et qu’une actualisation est en cours sur une autre réplique, attend la fin de cette actualisation.

```sql
SYSTEM WAIT VIEW [db.]name
```

<div id="cancel-view">
  ### SYSTEM CANCEL VIEW
</div>

Si une actualisation est en cours pour la vue donnée sur la réplique courante, interrompez-la et annulez-la. Sinon, ne faites rien.

```sql
SYSTEM CANCEL VIEW [db.]name
```

<div id="flush-object-storage-queue">
  ## SYSTEM FLUSH OBJECT STORAGE QUEUE
</div>

Bloque l’exécution jusqu’à ce que le fichier donné soit traité ou marqué comme définitivement en échec par la table [S3Queue](../../engines/table-engines/integrations/s3queue.md) ou [AzureQueue](../../engines/table-engines/integrations/azure-queue.md) donnée. Renvoie immédiatement si le fichier a déjà été traité. Déclenche une erreur si le fichier est définitivement en échec (toutes les tentatives ayant été épuisées).

```sql
SYSTEM FLUSH OBJECT STORAGE QUEUE [db.]table_name PATH 'path'
```