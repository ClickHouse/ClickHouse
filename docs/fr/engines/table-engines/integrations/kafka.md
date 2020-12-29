---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: Kafka
---

# Kafka {#kafka}

Ce moteur fonctionne avec [Apache Kafka](http://kafka.apache.org/).

Kafka vous permet de:

-   Publier ou s'abonner aux flux de données.
-   Organiser le stockage tolérant aux pannes.
-   Traiter les flux à mesure qu'ils deviennent disponibles.

## Création d'une Table {#table_engine-kafka-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_row_delimiter = 'delimiter_symbol',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0]
```

Les paramètres requis:

-   `kafka_broker_list` – A comma-separated list of brokers (for example, `localhost:9092`).
-   `kafka_topic_list` – A list of Kafka topics.
-   `kafka_group_name` – A group of Kafka consumers. Reading margins are tracked for each group separately. If you don't want messages to be duplicated in the cluster, use the same group name everywhere.
-   `kafka_format` – Message format. Uses the same notation as the SQL `FORMAT` la fonction, tels que `JSONEachRow`. Pour plus d'informations, voir le [Format](../../../interfaces/formats.md) section.

Paramètres facultatifs:

-   `kafka_row_delimiter` – Delimiter character, which ends the message.
-   `kafka_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) nécessite le chemin d'accès du fichier de schéma et le nom de la racine `schema.capnp:Message` objet.
-   `kafka_num_consumers` – The number of consumers per table. Default: `1`. Spécifiez plus de consommateurs si le débit d'un consommateur est insuffisant. Le nombre total de consommateurs ne doit pas dépasser le nombre de partitions dans la rubrique, car un seul consommateur peut être affecté par partition.
-   `kafka_max_block_size` - La taille maximale du lot (dans les messages) pour le sondage (par défaut: `max_block_size`).
-   `kafka_skip_broken_messages` – Kafka message parser tolerance to schema-incompatible messages per block. Default: `0`. Si `kafka_skip_broken_messages = N` puis le moteur saute *N* Messages Kafka qui ne peuvent pas être analysés (un message est égal à une ligne de données).
-   `kafka_commit_every_batch` - Commit chaque lot consommé et traité au lieu d'un seul commit après avoir écrit un bloc entier (par défaut: `0`).

Exemple:

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092',
                            kafka_topic_list = 'topic',
                            kafka_group_name = 'group1',
                            kafka_format = 'JSONEachRow',
                            kafka_num_consumers = 4;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">

<summary>Méthode obsolète pour créer une Table</summary>

!!! attention "Attention"
    N'utilisez pas cette méthode dans les nouveaux projets. Si possible, optez anciens projets à la méthode décrite ci-dessus.

``` sql
Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
      [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_skip_broken_messages])
```

</details>

## Description {#description}

Les messages livrés sont suivis automatiquement, de sorte que chaque message d'un groupe n'est compté qu'une seule fois. Si vous souhaitez obtenir les données deux fois, créez une copie de la table avec un autre nom de groupe.

Les groupes sont flexibles et synchronisés sur le cluster. Par exemple, si vous avez 10 thèmes et 5 copies d'une table dans un cluster, chaque copie obtient 2 sujets. Si le nombre de copies change, les rubriques sont redistribuées automatiquement entre les copies. En savoir plus à ce sujet à http://kafka.apache.org/intro.

`SELECT` n'est pas particulièrement utile pour la lecture de messages (sauf pour le débogage), car chaque message ne peut être lu qu'une seule fois. Il est plus pratique de créer des threads en temps réel à l'aide de vues matérialisées. Pour ce faire:

1.  Utilisez le moteur pour créer un consommateur Kafka et considérez-le comme un flux de données.
2.  Créez une table avec la structure souhaitée.
3.  Créer une vue matérialisée qui convertit les données du moteur et le met dans une table créée précédemment.

Lorsque l' `MATERIALIZED VIEW` rejoint le moteur, il commence à collecter des données en arrière-plan. Cela vous permet de recevoir continuellement des messages de Kafka et de les convertir au format requis en utilisant `SELECT`.
Une table kafka peut avoir autant de vues matérialisées que vous le souhaitez, elles ne lisent pas directement les données de la table kafka, mais reçoivent de nouveaux enregistrements( en blocs), de cette façon vous pouvez écrire sur plusieurs tables avec différents niveaux de détail (avec regroupement - agrégation et sans).

Exemple:

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() as total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

Pour améliorer les performances, les messages reçus sont regroupées en blocs de la taille de [max\_insert\_block\_size](../../../operations/server-configuration-parameters/settings.md#settings-max_insert_block_size). Si le bloc n'a pas été formé à l'intérieur [stream\_flush\_interval\_ms](../../../operations/server-configuration-parameters/settings.md) millisecondes, les données seront vidées dans le tableau, indépendamment de l'intégralité du bloc.

Pour arrêter de recevoir des données de rubrique ou pour modifier la logique de conversion, détachez la vue matérialisée:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Si vous souhaitez modifier la table cible en utilisant `ALTER`, nous vous recommandons de désactiver la vue matériel pour éviter les divergences entre la table cible et les données de la vue.

## Configuration {#configuration}

Similaire à GraphiteMergeTree, le moteur Kafka prend en charge la configuration étendue à l'aide du fichier de configuration ClickHouse. Il y a deux clés de configuration que vous pouvez utiliser: global (`kafka`) et des rubriques (`kafka_*`). La configuration globale est appliquée en premier, puis la configuration au niveau de la rubrique est appliquée (si elle existe).

``` xml
  <!-- Global configuration options for all tables of Kafka engine type -->
  <kafka>
    <debug>cgrp</debug>
    <auto_offset_reset>smallest</auto_offset_reset>
  </kafka>

  <!-- Configuration specific for topic "logs" -->
  <kafka_logs>
    <retry_backoff_ms>250</retry_backoff_ms>
    <fetch_min_bytes>100000</fetch_min_bytes>
  </kafka_logs>
```

Pour obtenir une liste des options de configuration possibles, consultez [librdkafka référence de configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). Utilisez le trait de soulignement (`_`) au lieu d'un point dans la configuration ClickHouse. Exemple, `check.crcs=true` sera `<check_crcs>true</check_crcs>`.

## Les Colonnes Virtuelles {#virtual-columns}

-   `_topic` — Kafka topic.
-   `_key` — Key of the message.
-   `_offset` — Offset of the message.
-   `_timestamp` — Timestamp of the message.
-   `_partition` — Partition of Kafka topic.

**Voir Aussi**

-   [Les colonnes virtuelles](../index.md#table_engines-virtual_columns)

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/kafka/) <!--hide-->
