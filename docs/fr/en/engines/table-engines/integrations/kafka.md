---
description: 'Le moteur de table Kafka fonctionne avec Apache Kafka et vous permet de publier ou de vous abonner
  à des flux de données, de mettre en place un stockage tolérant aux pannes et de traiter les flux à mesure qu''ils deviennent
  disponibles.'
sidebar_label: 'Kafka'
sidebar_position: 110
slug: /engines/table-engines/integrations/kafka
title: 'Moteur de table Kafka'
keywords: ['Kafka', 'moteur de table']
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="kafka-table-engine">
  # moteur de table Kafka
</div>

:::tip
Si vous utilisez ClickHouse Cloud, nous vous recommandons plutôt [ClickPipes](/fr/integrations/clickpipes). ClickPipes prend nativement en charge les connexions via un réseau privé, la mise à l&#39;échelle indépendante de l&#39;ingestion et des ressources du cluster, ainsi qu&#39;un monitoring complet pour l&#39;ingestion en streaming de données Kafka dans ClickHouse.
:::

* Publier ou s&#39;abonner à des flux de données.
* Mettre en place un stockage tolérant aux pannes.
* Traiter les flux à mesure qu&#39;ils deviennent disponibles.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [ALIAS expr1],
    name2 [type2] [ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_security_protocol = '',]
    [kafka_sasl_mechanism = '',]
    [kafka_sasl_username = '',]
    [kafka_sasl_password = '',]
    [kafka_autodetect_client_rack = '',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0,]
    [kafka_client_id = '',]
    [kafka_poll_timeout_ms = 0,]
    [kafka_poll_max_batch_size = 0,]
    [kafka_flush_interval_ms = 0,]
    [kafka_consumer_reschedule_ms = 0,]
    [kafka_thread_per_consumer = 0,]
    [kafka_handle_error_mode = 'default',]
    [kafka_commit_on_select = false,]
    [kafka_consumer_acquire_timeout_ms = 30000,]
    [kafka_max_rows_per_message = 1,]
    [kafka_compression_codec = '',]
    [kafka_compression_level = -1];
```

Paramètres obligatoires :

* `kafka_broker_list` — Une liste de brokers séparés par des virgules (par exemple, `localhost:9092`).
* `kafka_topic_list` — Une liste de topics Kafka.
* `kafka_group_name` — Un groupe de consommateurs Kafka. Les offsets de lecture sont suivis séparément pour chaque groupe. Si vous ne voulez pas que les messages soient dupliqués dans le cluster, utilisez le même nom de groupe partout.
* `kafka_format` — Format des messages. Utilise la même notation que la fonction SQL `FORMAT`, par exemple `JSONEachRow`. Pour plus d&#39;informations, consultez la section [Formats](../../../interfaces/formats.md).

Paramètres facultatifs :

* `kafka_security_protocol` - Protocole utilisé pour communiquer avec les brokers. Valeurs possibles : `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`.
* `kafka_sasl_mechanism` - Mécanisme SASL à utiliser pour l’authentification. Valeurs possibles : `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `AWS_MSK_IAM`.
* `kafka_aws_region` - Région AWS pour l’authentification MSK IAM. Détectée automatiquement à partir de l’adresse du broker si elle n’est pas spécifiée. Indiquez-la explicitement lorsque vous utilisez des alias PrivateLink ou des noms d’hôte DNS personnalisés qui ne contiennent pas d’informations de région. Par défaut : vide (détection automatique).
* `kafka_sasl_username` - Nom d’utilisateur SASL à utiliser avec les mécanismes `PLAIN` et `SASL-SCRAM-..`.
* `kafka_sasl_password` - Mot de passe SASL à utiliser avec les mécanismes `PLAIN` et `SASL-SCRAM-..`.
* `kafka_schema` — Paramètre à utiliser si le format nécessite une définition de schéma. Par exemple, [Cap&#39;n Proto](https://capnproto.org/) requiert le chemin du fichier de schéma ainsi que le nom de l’objet racine `schema.capnp:Message`.
* `kafka_schema_registry_skip_bytes` — Nombre d’octets à ignorer au début de chaque message lors de l’utilisation d’un schema registry avec des en-têtes d’enveloppe (par ex. AWS Glue Schema Registry, qui inclut une enveloppe de 19 octets). Plage : `[0, 255]`. Par défaut : `0`.
* `kafka_num_consumers` — Nombre de consumers par table. Indiquez davantage de consumers si le throughput d’un consumer est insuffisant. Le nombre total de consumers ne doit pas dépasser le nombre de partitions du topic, puisqu’un seul consumer peut être affecté à chaque partition, ni être supérieur au nombre de cœurs physiques du serveur sur lequel ClickHouse est déployé. Par défaut : `1`.
* `kafka_max_block_size` — Taille maximale du batch (en messages) pour le poll. Par défaut : [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `kafka_skip_broken_messages` — Tolérance de l’analyseur de messages Kafka aux messages incompatibles avec le schéma, par bloc. Si `kafka_skip_broken_messages = N`, le moteur ignore *N* messages Kafka qui ne peuvent pas être analysés (un message équivaut à une ligne de données). Par défaut : `0`.
* `kafka_commit_every_batch` — Effectue un commit pour chaque batch consommé et traité, au lieu d’un seul commit après l’écriture d’un bloc entier. Par défaut : `0`.
* `kafka_client_id` — Identifiant du client. Vide par défaut.
* `kafka_poll_timeout_ms` — Timeout pour un poll Kafka unique. Par défaut : [stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
* `kafka_poll_max_batch_size` — Nombre maximal de messages récupérés lors d’un seul poll Kafka. Par défaut : [max&#95;block&#95;size](/fr/operations/settings/settings#max_block_size).
* `kafka_flush_interval_ms` — Timeout pour le flush des données depuis Kafka. Par défaut : [stream&#95;flush&#95;interval&#95;ms](/fr/operations/settings/settings#stream_flush_interval_ms).
* `kafka_consumer_reschedule_ms` — Intervalle de replanification lorsque le stream processing Kafka est bloqué (par ex. lorsqu’aucun message n’est disponible à la consommation). Ce paramètre contrôle le délai avant que le consumer ne réessaie d’effectuer un poll. Ne doit pas dépasser `kafka_consumers_pool_ttl_ms`. Par défaut : `500` millisecondes.
* `kafka_thread_per_consumer` — Fournit un thread indépendant pour chaque consumer. Lorsqu’il est activé, chaque consumer flush les données indépendamment, en parallèle (sinon, les lignes de plusieurs consumers sont fusionnées pour former un bloc). Par défaut : `0`.
* `kafka_handle_error_mode` — Mode de gestion des erreurs pour le moteur Kafka. Valeurs possibles : default (une exception est levée si l’analyse d’un message échoue), stream (le message d’exception et le message brut sont enregistrés dans les colonnes virtuelles `_error` et `_raw_message`), dead&#95;letter&#95;queue (les données liées à l’erreur sont enregistrées dans system.dead&#95;letter&#95;queue).
* `kafka_commit_on_select` —  Effectue un commit des messages lorsqu’une requête `SELECT` est exécutée. Par défaut : `false`.
* `kafka_consumer_acquire_timeout_ms` — Timeout, en millisecondes, pour obtenir un consumer Kafka lors de requêtes `SELECT` directes sur une table `Kafka2` (avec stockage des offsets basé sur Keeper). Lorsque plusieurs requêtes `SELECT` directes concurrentes s’exécutent sur la même table, chacune doit attendre qu’un consumer devienne disponible. Ce timeout évite les deadlocks lorsque des requêtes détiennent différents sous-ensembles de consumers. Par défaut : `30000`.
* `kafka_max_rows_per_message` — Le nombre maximal de lignes écrites dans un message Kafka pour les formats basés sur les lignes. Par défaut : `1`.
* `kafka_autodetect_client_rack` — Définit automatiquement le paramètre `client.rack` pour `librdkafka` afin de privilégier les répliques Kafka les plus proches.
  Sources prises en charge :
  `AWS_ZONE_ID` pour l’ID de zone de disponibilité AWS IMDSv2, par exemple `euc1-az1` ;
  `AWS_ZONE_NAME` pour le nom de zone de disponibilité AWS IMDSv2, par exemple `eu-central-1a` ;
  `GCP_ZONE` pour la zone du service de métadonnées GCP, par exemple `europe-central2-a` ;
  `CLICKHOUSE` pour utiliser la détection interne de ClickHouse, qui peut s’appuyer sur les métadonnées cloud ou sur la configuration ;
  `AWS_ZONE_NAME_THEN_GCP_ZONE` pour essayer `AWS_ZONE_NAME`, puis `GCP_ZONE`.
  Par défaut : chaîne vide, désactivé.
  Conseil : les environnements n’utilisent pas tous le même format de zone de disponibilité. Amazon MSK utilise généralement des ID de zone ; préférez donc `AWS_ZONE_ID`. Confluent Cloud utilise généralement des noms de zone ; préférez donc `AWS_ZONE_NAME`. En cas de doute, utilisez `AWS_ZONE_NAME_THEN_GCP_ZONE` ou vérifiez la valeur `broker.rack` sur votre cluster.
  Remarque : les brokers Kafka doivent être configurés avec `broker.rack` et `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector`.
* `kafka_compression_codec` — Codec de compression utilisé pour produire les messages. Pris en charge : chaîne vide, `none`, `gzip`, `snappy`, `lz4`, `zstd`. Si la chaîne est vide, le codec de compression n’est pas défini par la table ; les valeurs des fichiers de configuration ou la valeur par défaut de `librdkafka` seront donc utilisées. Par défaut : chaîne vide.
* `kafka_compression_level` — Paramètre de niveau de compression pour l’algorithme sélectionné par kafka&#95;compression&#95;codec. Des valeurs plus élevées offrent une meilleure compression, au prix d’une utilisation CPU plus importante. La plage utilisable dépend de l’algorithme : `[0-9]` pour `gzip` ; `[0-12]` pour `lz4` ; uniquement `0` pour `snappy` ; `[0-12]` pour `zstd` ; `-1` = niveau de compression par défaut dépendant du codec. Par défaut : `-1`.
* `kafka_map_virtual_columns_on_write` — Si activé, les colonnes portant les noms spéciaux `_key`, `_timestamp`, `_headers.name` et `_headers.value` dans le schéma de la table sont associées aux métadonnées correspondantes du message Kafka lors de `INSERT` et sont exclues de la charge utile du message. Voir [Mappage des colonnes aux métadonnées des messages Kafka](#mapping-columns-to-kafka-message-metadata). Par défaut : `false`.

Exemples :

```sql
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

  CREATE TABLE queue3 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">
  <summary>Méthode obsolète pour créer une table</summary>

  :::note
  N’utilisez pas cette méthode dans les nouveaux projets. Si possible, migrez les anciens projets vers la méthode décrite ci-dessus.
  :::

  ```sql
  Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
        [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_max_block_size,  kafka_skip_broken_messages, kafka_commit_every_batch, kafka_client_id, kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms, kafka_consumer_reschedule_ms, kafka_thread_per_consumer, kafka_handle_error_mode, kafka_commit_on_select, kafka_max_rows_per_message]);
  ```
</details>

:::info
Le moteur de table Kafka ne prend pas en charge les colonnes dotées d’une [valeur par défaut](/fr/sql-reference/statements/create/table#default_values). Si vous avez besoin de colonnes avec une valeur par défaut, vous pouvez les ajouter au niveau de la vue matérialisée (voir ci-dessous).
:::

<div id="description">
  ## Description
</div>

Les messages distribués sont suivis automatiquement, de sorte que chaque message d’un groupe n’est compté qu’une seule fois. Si vous souhaitez lire les données deux fois, créez une copie de la table avec un autre nom de groupe.

Les groupes sont flexibles et synchronisés dans le cluster. Par exemple, si vous avez 10 topics et 5 copies d’une table dans un cluster, chaque copie reçoit 2 topics. Si le nombre de copies change, les topics sont automatiquement redistribués entre les copies. Pour en savoir plus à ce sujet, consultez http://kafka.apache.org/intro.

Il est recommandé que chaque topic Kafka ait son propre groupe de consommateurs dédié, afin de garantir une association exclusive entre le topic et le groupe, en particulier dans les environnements où les topics peuvent être créés et supprimés dynamiquement (par ex., en test ou en staging).

`SELECT` n’est pas particulièrement utile pour lire les messages (sauf pour le débogage), car chaque message ne peut être lu qu’une seule fois. Il est plus pratique de créer des flux en temps réel à l’aide de vues matérialisées. Pour ce faire :

1. Utilisez le moteur pour créer un consumer Kafka et traitez-le comme un flux de données.
2. Créez une table avec la structure souhaitée.
3. Créez une vue matérialisée qui convertit les données du moteur et les insère dans une table créée précédemment.

Lorsque la `MATERIALIZED VIEW` est liée au moteur, elle commence à collecter les données en arrière-plan. Cela vous permet de recevoir continuellement des messages de Kafka et de les convertir au format requis à l’aide de `SELECT`.
Une table Kafka peut avoir autant de vues matérialisées que vous le souhaitez ; elles ne lisent pas directement les données de la table Kafka, mais reçoivent de nouveaux enregistrements (par blocs). Vous pouvez ainsi écrire dans plusieurs tables avec différents niveaux de granularité (avec regroupement/agrégation ou sans).

Exemple :

```sql
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
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

Pour améliorer les performances, les messages reçus sont regroupés en blocs de la taille de [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size). Si le bloc n&#39;est pas constitué dans un délai de [stream&#95;flush&#95;interval&#95;ms](/fr/operations/settings/settings#stream_flush_interval_ms) millisecondes, les données sont écrites dans la table même si le bloc n&#39;est pas complet.

Pour cesser de recevoir les données du topic ou modifier la logique de conversion, détachez la vue matérialisée :

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Si vous souhaitez modifier la table cible en utilisant `ALTER`, nous vous recommandons de désactiver la vue matérialisée afin d’éviter toute incohérence entre la table cible et les données de la vue.

<div id="configuration">
  ## Configuration
</div>

À l’instar de GraphiteMergeTree, le moteur Kafka prend en charge une configuration étendue via le fichier de configuration de ClickHouse. Deux clés de configuration sont disponibles : une clé globale (sous `<kafka>`) et une clé au niveau du topic (sous `<kafka><kafka_topic>`). La configuration globale est appliquée en premier, puis celle au niveau du topic l’est ensuite (si elle existe).

```xml
  <kafka>
    <!-- Global configuration options for all tables of Kafka engine type -->
    <debug>cgrp</debug>
    <statistics_interval_ms>3000</statistics_interval_ms>

    <kafka_topic>
        <name>logs</name>
        <statistics_interval_ms>4000</statistics_interval_ms>
    </kafka_topic>

    <!-- Settings for consumer -->
    <consumer>
        <auto_offset_reset>smallest</auto_offset_reset>
        <kafka_topic>
            <name>logs</name>
            <fetch_min_bytes>100000</fetch_min_bytes>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <fetch_min_bytes>50000</fetch_min_bytes>
        </kafka_topic>
    </consumer>

    <!-- Settings for producer -->
    <producer>
        <kafka_topic>
            <name>logs</name>
            <retry_backoff_ms>250</retry_backoff_ms>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <retry_backoff_ms>400</retry_backoff_ms>
        </kafka_topic>
    </producer>
  </kafka>
```

Pour la liste des options de configuration disponibles, consultez la [référence de configuration de librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). Utilisez le caractère de soulignement (`_`) au lieu d’un point dans la configuration de ClickHouse. Par exemple, `check.crcs=true` devient `<check_crcs>true</check_crcs>`.

<div id="kafka-aws-msk-iam">
  ### Authentification IAM pour AWS MSK
</div>

:::note
L’authentification IAM pour AWS MSK nécessite que ClickHouse soit compilé avec la prise en charge de S3 activée.
:::

AWS MSK prend en charge l’authentification basée sur IAM, ce qui permet de se connecter à des clusters Kafka à l’aide d’identifiants AWS au lieu de gérer des noms d’utilisateur et mots de passe distincts.

**Configuration de base :**

Définissez `kafka_sasl_mechanism = 'AWS_MSK_IAM'` dans les paramètres de table :

```sql
CREATE TABLE msk_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'b-1.mycluster.kafka.us-east-1.amazonaws.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM';
```

La région AWS est automatiquement extraite de l’endpoint du broker par correspondance de motifs :

* MSK provisionné : `b-X.cluster.kafka.<region>.amazonaws.com:9098`
* MSK serverless : `boot-X.kafka-serverless.<region>.amazonaws.com:9098`
* VPC Endpoint : `vpce-X.kafka.<region>.vpce.amazonaws.com:9098`

**Identifiants AWS :**

Les identifiants sont toujours chargés depuis `~/.aws/credentials` et `~/.aws/config` (fichiers de profil AWS) lorsqu’ils sont présents. Pour activer également les profils d’instance EC2, les variables d’environnement (`AWS_ACCESS_KEY_ID`, etc.), les rôles de tâche ECS et les autres sources automatiques d’identifiants, ajoutez ce qui suit à la configuration de votre serveur :

```xml
<kafka>
  <use_environment_credentials>true</use_environment_credentials>
</kafka>
```

Ce paramètre peut uniquement être configuré par les administrateurs du serveur. Par défaut : `false`.

**PrivateLink et DNS personnalisé :**

Lorsque vous utilisez des alias PrivateLink ou des hostnames DNS personnalisés qui ne contiennent pas d’informations sur la région, indiquez explicitement la région AWS :

```sql
CREATE TABLE msk_privatelink_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'my-privatelink-alias.internal.example.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM',
    kafka_aws_region = 'us-east-1';
```

**Autorisations IAM :**

Autorisations du consommateur (pour la lecture des messages) :

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:group/CLUSTER_NAME/CONSUMER_GROUP/*"
    ]
  }]
}
```

Autorisations du producteur (écriture de messages) :

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*"
    ]
  }]
}
```

<div id="kafka-kerberos-support">
  ### Prise en charge de Kerberos
</div>

Pour utiliser Kafka avec prise en charge de Kerberos, ajoutez l’élément enfant `security_protocol` avec la valeur `sasl_plaintext`. Cela suffit si le ticket d’octroi de tickets Kerberos est obtenu et mis en cache par le système d’exploitation.
ClickHouse peut gérer les informations d’authentification Kerberos à l’aide d’un fichier keytab. Utilisez les éléments enfants `sasl_kerberos_service_name`, `sasl_kerberos_keytab` et `sasl_kerberos_principal`.

Exemple :

```xml
<!-- Kerberos-aware Kafka -->
<kafka>
  <security_protocol>SASL_PLAINTEXT</security_protocol>
  <sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
  <sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
</kafka>
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_topic` — Topic Kafka. Type de données : `LowCardinality(String)`.
* `_key` — Clé du message. Type de données : `String`.
* `_offset` — Offset du message. Type de données : `UInt64`.
* `_timestamp` — Horodatage du message. Type de données : `Nullable(DateTime)`.
* `_timestamp_ms` — Horodatage du message en millisecondes. Type de données : `Nullable(DateTime64(3))`.
* `_partition` — Partition du topic Kafka. Type de données : `UInt64`.
* `_headers.name` — Tableau des clés d&#39;en-tête du message. Type de données : `Array(String)`.
* `_headers.value` — Tableau des valeurs d&#39;en-tête du message. Type de données : `Array(String)`.

Colonnes virtuelles supplémentaires lorsque `kafka_handle_error_mode='stream'` :

* `_raw_message` - Message brut qui n&#39;a pas pu être analysé correctement. Type de données : `String`.
* `_error` - Message d&#39;exception survenu lors de l&#39;échec de l&#39;analyse. Type de données : `String`.

Remarque : les colonnes virtuelles `_raw_message` et `_error` ne sont renseignées qu&#39;en cas d&#39;exception lors de l&#39;analyse ; elles sont toujours vides lorsque le message a été analysé correctement.

<div id="mapping-columns-to-kafka-message-metadata">
  ## Correspondance des colonnes aux métadonnées des messages Kafka
</div>

Lors de la production de messages avec `INSERT INTO`, le moteur Kafka utilise toujours une colonne nommée `_key` (de type `String`) comme clé du message Kafka et une colonne nommée `_timestamp` (de type `DateTime`) comme horodatage du message Kafka — si ces colonnes existent dans la table. Par défaut, ces colonnes apparaissent également dans le payload du message produit, aux côtés des autres colonnes.

Avec `kafka_map_virtual_columns_on_write = 1`, le comportement change :

* `_key` (type `String`) — mappé à la clé du message Kafka.
* `_timestamp` (type `DateTime`) — mappé à l’horodatage du message Kafka.
* `_headers.name` (type `Array(String)`) et `_headers.value` (type `Array(String)`) — mappés aux en-têtes du message Kafka. Chaque paire `(_headers.name[i], _headers.value[i])` devient un en-tête Kafka. Comme `_headers.name` et `_headers.value` partagent le préfixe Nested `_headers`, ClickHouse exige que les deux tableaux aient la même taille pour chaque ligne.

Les colonnes portant ces noms sont **exclues du payload du message** uniquement si leurs types correspondent à ceux indiqués ci-dessus ; sinon, elles restent dans le payload, de sorte que les schémas qui réutilisent ces noms par hasard pour des données sans rapport continuent de fonctionner.

Exemple :

```sql
CREATE TABLE kafka_out
(
    event_json String,
    `_key` String,
    `_timestamp` DateTime,
    `_headers.name` Array(String),
    `_headers.value` Array(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'events-producer',
    kafka_format = 'JSONEachRow',
    kafka_map_virtual_columns_on_write = 1;

INSERT INTO kafka_out VALUES
    ('{"a":1}', 'session-42', now(), ['source', 'trace_id'], ['api', 'abc-123']);
```

Le message Kafka généré contient la charge utile `{"event_json":"{\"a\":1}"}`, la clé `session-42`, l’horodatage actuel et deux en-têtes `source=api` et `trace_id=abc-123`.

<div id="data-formats-support">
  ## Prise en charge des formats de données
</div>

Le moteur Kafka prend en charge tous les [formats](../../../interfaces/formats.md) pris en charge par ClickHouse.
Le nombre de lignes dans un message Kafka dépend du fait que le format soit basé sur les lignes ou sur les blocs :

* Pour les formats basés sur les lignes, le nombre de lignes dans un message Kafka peut être contrôlé en définissant `kafka_max_rows_per_message`.
* Pour les formats basés sur les blocs, il n’est pas possible de diviser un bloc en parties plus petites, mais le nombre de lignes dans un bloc peut être contrôlé par le paramètre général [max&#95;block&#95;size](/fr/operations/settings/settings#max_block_size).

<div id="engine-to-store-committed-offsets-in-clickhouse-keeper">
  ## Moteur permettant de stocker les offsets commités dans ClickHouse Keeper
</div>

<ExperimentalBadge />

Si `allow_experimental_kafka_offsets_storage_in_keeper` est activé, deux paramètres supplémentaires peuvent être spécifiés pour le moteur de table Kafka :

* `kafka_keeper_path` indique le chemin vers la table dans ClickHouse Keeper
* `kafka_replica_name` indique le nom de la réplique dans ClickHouse Keeper

Soit les deux paramètres doivent être spécifiés, soit aucun des deux. Lorsque les deux sont spécifiés, un nouveau moteur Kafka expérimental est utilisé. Ce nouveau moteur ne dépend pas du stockage des offsets commités dans Kafka : il les stocke dans ClickHouse Keeper. Il tente toujours de commit les offsets dans Kafka, mais il ne s’appuie sur eux qu’au moment de la création de la table. Dans tous les autres cas (redémarrage de la table ou récupération après une erreur), les offsets stockés dans ClickHouse Keeper sont utilisés pour reprendre la consommation des messages. En plus de l’offset commité, il stocke également le nombre de messages consommés dans le dernier batch ; ainsi, si l’insertion échoue, le même nombre de messages sera consommé, ce qui permet la déduplication si nécessaire.

Exemple :

```sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/{uuid}',
  kafka_replica_name = '{replica}'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

<div id="known-limitations">
  ### Limites connues
</div>

Comme le nouveau moteur est expérimental, il n&#39;est pas encore prêt pour la production. L&#39;implémentation présente quelques limites connues :

* Supprimer puis recréer rapidement la table, ou spécifier le même chemin ClickHouse Keeper pour différents moteurs, peut entraîner des problèmes. Comme bonne pratique, vous pouvez utiliser `{uuid}` dans `kafka_keeper_path` pour éviter les conflits de chemins.
* Pour garantir des lectures répétables, les messages ne peuvent pas être consommés à partir de plusieurs partitions sur un seul thread. En revanche, les consommateurs Kafka doivent être interrogés régulièrement pour rester actifs. En raison de ces deux contraintes, nous avons décidé de n&#39;autoriser la création de plusieurs consommateurs que si `kafka_thread_per_consumer` est activé ; sinon, il est trop compliqué d&#39;éviter les problèmes liés à leur interrogation régulière.

**Voir aussi**

* [Colonnes virtuelles](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* [background&#95;message&#95;broker&#95;schedule&#95;pool&#95;size](/fr/operations/server-configuration-parameters/settings#background_message_broker_schedule_pool_size)
* [system.kafka&#95;consumers](../../../operations/system-tables/kafka_consumers.md)