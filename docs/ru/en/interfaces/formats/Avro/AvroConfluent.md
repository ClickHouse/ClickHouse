---
alias: []
description: 'Документация для формата AvroConfluent'
input_format: true
keywords: ['AvroConfluent']
output_format: true
slug: /interfaces/formats/AvroConfluent
title: 'AvroConfluent'
doc_type: 'reference'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

[Apache Avro](https://avro.apache.org/) — это построчный формат сериализации, использующий двоичное кодирование для эффективной обработки данных. Формат `AvroConfluent` поддерживает чтение и запись сообщений в кодировке Avro с использованием [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) (или сервисов с совместимым API).

В каждом сообщении используется формат передачи данных Confluent: magic-байт (`0x00`), затем 4-байтовый ID схемы в порядке байтов big-endian, после чего следует двоичное значение Avro. При чтении ClickHouse определяет ID схемы, обращаясь к реестру. При записи ClickHouse регистрирует схему, полученную из выходных столбцов, и добавляет полученный ID в начало каждой строки. Для оптимальной производительности схемы кэшируются.

<a id="data-types-matching" />

<div id="data-type-mapping">
  ## Соответствие типов данных
</div>

<DataTypesMatching />

<div id="format-settings">
  ## Настройки формата
</div>

[//]: # "ПРИМЕЧАНИЕ: Эти настройки можно задавать на уровне сеанса, но это встречается нечасто, и если акцентировать на этом слишком сильно, пользователи могут запутаться."

| Настройка                                        | Описание                                                                                                                                                                      | По умолчанию |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `input_format_avro_allow_missing_fields`         | Использовать ли значение по умолчанию вместо генерации ошибки, если поле не найдено в схеме.                                                                                  | `0`          |
| `input_format_avro_null_as_default`              | Использовать ли значение по умолчанию вместо генерации ошибки при вставке значения `null` в столбец, не допускающий `NULL`.                                                   | `0`          |
| `format_avro_schema_registry_url`                | URL Confluent Schema Registry. Для базовой аутентификации в путь URL можно напрямую включить учетные данные, закодированные для URL.                                          |              |
| `format_avro_schema_registry_connection_timeout` | Тайм-аут подключения в секундах для HTTP-клиента Schema Registry (используется как для получения схемы, так и для регистрации). Должен быть больше 0 и меньше 600 (10 минут). | `1`          |
| `format_avro_schema_registry_send_timeout`       | Тайм-аут отправки в секундах для HTTP-клиента Schema Registry. Должен быть больше 0 и меньше 600 (10 минут).                                                                  | `1`          |
| `format_avro_schema_registry_receive_timeout`    | Тайм-аут получения в секундах для HTTP-клиента Schema Registry. Должен быть больше 0 и меньше 600 (10 минут).                                                                 | `1`          |
| `output_format_avro_confluent_subject`           | Для вывода: имя subject, под которым схема зарегистрирована в Schema Registry. Обязательно при записи.                                                                        |              |
| `output_format_avro_string_column_pattern`       | Для вывода: регулярное выражение для столбцов String, которые нужно сериализовать как Avro `string` (по умолчанию — `bytes`).                                                 |              |

<div id="examples">
  ## Примеры
</div>

<div id="reading-from-kafka">
  ### Чтение из Kafka
</div>

Чтобы читать Avro-кодированный топик Kafka с помощью [движка таблицы Kafka](/ru/engines/table-engines/integrations/kafka.md), используйте настройку `format_avro_schema_registry_url`, указав URL реестра схем.

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
  ### Запись в Kafka
</div>

Чтобы записывать сообщения AvroConfluent в топик Kafka, задайте URL реестра схем и имя subject. При первой записи схема автоматически регистрируется в реестре.

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
  #### Использование базовой аутентификации
</div>

Если ваш реестр схем требует базовой аутентификации (например, если вы используете Confluent Cloud), вы можете указать учётные данные в URL-кодировке в параметре `format_avro_schema_registry_url`.

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
  ## Устранение неполадок
</div>

Чтобы отслеживать ход ингестии и диагностировать ошибки потребителя Kafka, вы можете выполнить запрос к [системной таблице `system.kafka_consumers`](../../../operations/system-tables/kafka_consumers.md). Если в вашем развертывании несколько реплик (например, в ClickHouse Cloud), необходимо использовать [табличную функцию `clusterAllReplicas`](../../../sql-reference/table-functions/cluster.md).

```sql
SELECT * FROM clusterAllReplicas('default',system.kafka_consumers)
ORDER BY assignments.partition_id ASC;
```

Если у вас возникают проблемы с определением схемы, для диагностики можно использовать [kafkacat](https://github.com/edenhill/kafkacat) вместе с [clickhouse-local](/ru/operations/utilities/clickhouse-local.md):

```bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```