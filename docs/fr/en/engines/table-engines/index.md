---
description: 'Documentation sur les moteurs de table'
slug: /engines/table-engines/
toc_folder_title: 'Moteurs de table'
toc_priority: 26
toc_title: 'Introduction'
title: 'Moteurs de table'
doc_type: 'reference'
---

Le moteur de table (type de table) détermine :

* Comment et où les données sont stockées, où elles sont écrites et d’où elles sont lues.
* Quelles requêtes sont prises en charge, et de quelle manière.
* L’accès concurrent aux données.
* L’utilisation des index, le cas échéant.
* Si l’exécution multithread des requêtes est possible.
* Les paramètres de réplication des données.

<div id="engine-families">
  ## Familles de moteurs
</div>

<div id="mergetree">
  ### MergeTree
</div>

Les moteurs de table les plus universels et les plus complets pour les charges élevées. Leur caractéristique commune est l’insertion rapide des données, suivie de leur traitement en tâche de fond. Les moteurs de la famille `MergeTree` prennent en charge la réplication des données (avec les versions [Replicated*](/fr/engines/table-engines/mergetree-family/replication) des moteurs), le partitionnement, les index secondaires de data-skipping, ainsi que d’autres fonctionnalités non prises en charge par les autres moteurs.

Moteurs de la famille :

| Moteurs MergeTree                                                                                    |
| ---------------------------------------------------------------------------------------------------- |
| [MergeTree](/fr/engines/table-engines/mergetree-family/mergetree)                                       |
| [ReplacingMergeTree](/fr/engines/table-engines/mergetree-family/replacingmergetree)                     |
| [SummingMergeTree](/fr/engines/table-engines/mergetree-family/summingmergetree)                         |
| [AggregatingMergeTree](/fr/engines/table-engines/mergetree-family/aggregatingmergetree)                 |
| [CollapsingMergeTree](/fr/engines/table-engines/mergetree-family/collapsingmergetree)                   |
| [VersionedCollapsingMergeTree](/fr/engines/table-engines/mergetree-family/versionedcollapsingmergetree) |
| [GraphiteMergeTree](/fr/engines/table-engines/mergetree-family/graphitemergetree)                       |
| [CoalescingMergeTree](/fr/engines/table-engines/mergetree-family/coalescingmergetree)                   |

<div id="log">
  ### Log
</div>

Des [moteurs](../../engines/table-engines/log-family/index.md) légers offrant un minimum de fonctionnalités. Ils sont particulièrement efficaces lorsque vous devez écrire rapidement de nombreuses petites tables (jusqu&#39;à environ 1 million de lignes), puis les lire ensuite dans leur ensemble.

Moteurs de la famille :

| Moteurs Log                                              |
| -------------------------------------------------------- |
| [TinyLog](/fr/engines/table-engines/log-family/tinylog)     |
| [StripeLog](/fr/engines/table-engines/log-family/stripelog) |
| [Log](/fr/engines/table-engines/log-family/log)             |

<div id="integration-engines">
  ### Moteurs d’intégration
</div>

Moteurs permettant de communiquer avec d’autres systèmes de stockage et de traitement de données.

Moteurs de la famille :

| Moteurs d’intégration                                                           |
| ------------------------------------------------------------------------------- |
| [ODBC](../../engines/table-engines/integrations/odbc.md)                        |
| [JDBC](../../engines/table-engines/integrations/jdbc.md)                        |
| [MySQL](../../engines/table-engines/integrations/mysql.md)                      |
| [MongoDB](../../engines/table-engines/integrations/mongodb.md)                  |
| [Redis](../../engines/table-engines/integrations/redis.md)                      |
| [HDFS](../../engines/table-engines/integrations/hdfs.md)                        |
| [S3](../../engines/table-engines/integrations/s3.md)                            |
| [Kafka](../../engines/table-engines/integrations/kafka.md)                      |
| [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) |
| [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)                |
| [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)            |
| [S3Queue](../../engines/table-engines/integrations/s3queue.md)                  |
| [TimeSeries](../../engines/table-engines/integrations/time-series.md)           |

<div id="special-engines">
  ### Moteurs spéciaux
</div>

Moteurs de cette famille :

| Moteurs spéciaux                                              |
| ------------------------------------------------------------- |
| [Distributed](/fr/engines/table-engines/special/distributed)     |
| [Dictionary](/fr/engines/table-engines/special/dictionary)       |
| [Merge](/fr/engines/table-engines/special/merge)                 |
| [Executable](/fr/engines/table-engines/special/executable)       |
| [File](/fr/engines/table-engines/special/file)                   |
| [Null](/fr/engines/table-engines/special/null)                   |
| [Set](/fr/engines/table-engines/special/set)                     |
| [Join](/fr/engines/table-engines/special/join)                   |
| [URL](/fr/engines/table-engines/special/url)                     |
| [View](/fr/engines/table-engines/special/view)                   |
| [Memory](/fr/engines/table-engines/special/memory)               |
| [Buffer](/fr/engines/table-engines/special/buffer)               |
| [External Data](/fr/engines/table-engines/special/external-data) |
| [GenerateRandom](/fr/engines/table-engines/special/generate)     |
| [KeeperMap](/fr/engines/table-engines/special/keeper-map)        |
| [FileLog](/fr/engines/table-engines/special/filelog)             |

<div id="table_engines-virtual_columns">
  ## Colonnes virtuelles
</div>

Une colonne virtuelle est un attribut intrinsèque du moteur de table, défini dans le code source du moteur.

Vous ne devez pas spécifier de colonnes virtuelles dans la requête `CREATE TABLE`, et elles n&#39;apparaissent pas dans les résultats des requêtes `SHOW CREATE TABLE` et `DESCRIBE TABLE`. Les colonnes virtuelles sont également en lecture seule, vous ne pouvez donc pas y insérer de données.

Pour sélectionner des données depuis une colonne virtuelle, vous devez indiquer son nom dans la requête `SELECT`. `SELECT *` ne renvoie pas les valeurs des colonnes virtuelles.

Si vous créez une table avec une colonne portant le même nom qu&#39;une des colonnes virtuelles de la table, cette colonne virtuelle devient inaccessible. Nous ne recommandons pas de le faire. Pour éviter les conflits, les noms des colonnes virtuelles sont généralement préfixés par un trait de soulignement.

* `_table` — Contient le nom de la table depuis laquelle les données ont été lues. Type : [String](../../sql-reference/data-types/string.md).

  Quel que soit le moteur de table utilisé, chaque table inclut une colonne virtuelle universelle nommée `_table`.

  Lorsque vous interrogez une table avec le moteur de table Merge, vous pouvez définir des conditions constantes sur `_table` dans la clause `WHERE/PREWHERE` (par exemple, `WHERE _table='xyz'`). Dans ce cas, l&#39;opération de lecture n&#39;est effectuée que pour les tables où la condition sur `_table` est satisfaite ; la colonne `_table` agit donc comme un index.

  Avec des requêtes de la forme `SELECT ... FROM (... UNION ALL ...)`, vous pouvez déterminer de quelle table réelle proviennent les lignes renvoyées en spécifiant la colonne `_table`.