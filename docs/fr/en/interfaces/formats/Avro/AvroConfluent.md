---
alias: []
description: 'Documentation du format AvroConfluent'
input_format: true
keywords: ['AvroConfluent']
output_format: true
slug: /interfaces/formats/AvroConfluent
title: 'AvroConfluent'
doc_type: 'reference'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

[Apache Avro](https://avro.apache.org/) est un format de sérialisation orienté ligne qui utilise un encodage binaire pour un traitement efficace des données. Le format `AvroConfluent` prend en charge la lecture et l&#39;écriture de messages encodés en Avro à l&#39;aide de [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) (ou de services compatibles avec l&#39;API).

Chaque message utilise le format wire de Confluent : un octet magique (`0x00`), suivi d&#39;un ID de schéma big-endian sur 4 octets, puis de la donnée binaire Avro. Lors de la lecture, ClickHouse résout l&#39;ID de schéma en interrogeant le registre. Lors de l&#39;écriture, ClickHouse enregistre le schéma dérivé des colonnes de sortie et ajoute l&#39;ID obtenu au début de chaque ligne. Les schémas sont mis en cache pour des performances optimales.

<a id="data-types-matching" />

<div id="data-type-mapping">
  ## Correspondance des types de données
</div>

<DataTypesMatching />

<div id="format-settings">
  ## Paramètres du format
</div>

[//]: # "REMARQUE Ces paramètres peuvent être définis au niveau de la session, mais cela reste peu courant et le documenter trop en évidence peut prêter à confusion pour les utilisateurs."

| Paramètre                                        | Description                                                                                                                                                                                                                      | Par défaut |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `input_format_avro_allow_missing_fields`         | Indique s&#39;il faut utiliser une valeur par défaut au lieu de générer une erreur lorsqu&#39;un champ est introuvable dans le schéma.                                                                                           | `0`        |
| `input_format_avro_null_as_default`              | Indique s&#39;il faut utiliser une valeur par défaut au lieu de générer une erreur lors de l&#39;insertion d&#39;une valeur `null` dans une colonne non Nullable.                                                                | `0`        |
| `format_avro_schema_registry_url`                | L&#39;URL de Confluent Schema Registry. Pour l&#39;authentification HTTP Basic, des identifiants encodés dans l&#39;URL peuvent être incluses directement dans le chemin de l&#39;URL.                        |            |
| `format_avro_schema_registry_connection_timeout` | Délai d&#39;expiration de la connexion, en secondes, pour le client HTTP du Schema Registry (utilisé à la fois pour la récupération et l&#39;enregistrement du schéma). Doit être supérieur à 0 et inférieur à 600 (10 minutes). | `1`        |
| `format_avro_schema_registry_send_timeout`       | Délai d&#39;expiration d&#39;envoi, en secondes, pour le client HTTP du Schema Registry. Doit être supérieur à 0 et inférieur à 600 (10 minutes).                                                                                | `1`        |
| `format_avro_schema_registry_receive_timeout`    | Délai d&#39;expiration de réception, en secondes, pour le client HTTP du Schema Registry. Doit être supérieur à 0 et inférieur à 600 (10 minutes).                                                                               | `1`        |
| `output_format_avro_confluent_subject`           | En sortie : le nom du sujet sous lequel le schéma est enregistré dans le Schema Registry. Obligatoire lors de l&#39;écriture.                                                                                                    |            |
| `output_format_avro_string_column_pattern`       | En sortie : expression régulière des colonne de type String à sérialiser en tant que `string` Avro (la valeur par défaut est `bytes`).                                                                                           |            |

<div id="examples">
  ## Exemples
</div>

<div id="reading-from-kafka">
  ### Lecture depuis Kafka
</div>

Pour lire un topic Kafka encodé en Avro à l’aide du [moteur de table Kafka](/fr/engines/table-engines/integrations/kafka.md), utilisez le paramètre `format_avro_schema_registry_url` pour indiquer l’URL du registre de schémas.

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url';

SELECT * FROM topic1_stream;
```

<div id="writing-to-kafka">
  ### Écrire dans Kafka
</div>

Pour écrire des messages AvroConfluent dans un topic Kafka, définissez à la fois l’URL du registre de schémas et le nom du sujet. Le schéma est automatiquement enregistré dans le registre lors de la première écriture.

```sql
CREATE TABLE topic1_sink
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url',
output_format_avro_confluent_subject = 'topic1-value';

INSERT INTO topic1_sink VALUES ('hello', 'world');
```

<div id="using-basic-authentication">
  #### Utiliser l’authentification HTTP Basic
</div>

Si votre registre de schémas nécessite une authentification HTTP Basic (par exemple, si vous utilisez Confluent Cloud), vous pouvez fournir des identifiants encodés dans l’URL dans le paramètre `format_avro_schema_registry_url`.

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'https://<username>:<password>@schema-registry-url';
```

<div id="troubleshooting">
  ## Dépannage
</div>

Pour surveiller la progression de l’ingestion et déboguer les erreurs du consommateur Kafka, vous pouvez interroger la [table système `system.kafka_consumers`](../../../operations/system-tables/kafka_consumers.md). Si votre déploiement comporte plusieurs répliques (par ex. ClickHouse Cloud), vous devez utiliser la fonction de table [`clusterAllReplicas`](../../../sql-reference/table-functions/cluster.md).

```sql
SELECT * FROM clusterAllReplicas('default',system.kafka_consumers)
ORDER BY assignments.partition_id ASC;
```

Si vous rencontrez des problèmes de résolution du schéma, vous pouvez utiliser [kafkacat](https://github.com/edenhill/kafkacat) avec [clickhouse-local](/fr/operations/utilities/clickhouse-local.md) pour diagnostiquer le problème :

```bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```