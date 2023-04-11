---
sidebar_position: 8
sidebar_label: Kafka
---

# Kafka {#kafka}

Движок работает с [Apache Kafka](http://kafka.apache.org/).

Kafka позволяет:

-   Публиковать/подписываться на потоки данных.
-   Организовать отказоустойчивое хранилище.
-   Обрабатывать потоки по мере их появления.

## Создание таблицы {#table_engine-kafka-creating-a-table}

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
    [kafka_skip_broken_messages = N]
    [kafka_commit_every_batch = 0,]
    [kafka_thread_per_consumer = 0]
```

Обязательные параметры:

-   `kafka_broker_list` — перечень брокеров, разделенный запятыми (`localhost:9092`).
-   `kafka_topic_list` — перечень необходимых топиков Kafka.
-   `kafka_group_name` — группа потребителя Kafka. Отступы для чтения отслеживаются для каждой группы отдельно. Если необходимо, чтобы сообщения не повторялись на кластере, используйте везде одно имя группы.
-   `kafka_format` — формат сообщений. Названия форматов должны быть теми же, что можно использовать в секции `FORMAT`, например, `JSONEachRow`. Подробнее читайте в разделе [Форматы](../../../interfaces/formats.md).

Опциональные параметры:

-   `kafka_row_delimiter` — символ-разделитель записей (строк), которым завершается сообщение.
-   `kafka_schema` — опциональный параметр, необходимый, если используется формат, требующий определения схемы. Например, [Cap’n Proto](https://capnproto.org/) требует путь к файлу со схемой и название корневого объекта `schema.capnp:Message`.
-   `kafka_num_consumers` — количество потребителей (consumer) на таблицу. По умолчанию: `1`. Укажите больше потребителей, если пропускная способность одного потребителя недостаточна. Общее число потребителей не должно превышать количество партиций в топике, так как на одну партицию может быть назначено не более одного потребителя.
-   `kafka_max_block_size` — максимальный размер пачек (в сообщениях) для poll (по умолчанию `max_block_size`).
-   `kafka_skip_broken_messages` — максимальное количество некорректных сообщений в блоке. Если `kafka_skip_broken_messages = N`, то движок отбрасывает `N` сообщений Кафки, которые не получилось обработать. Одно сообщение в точности соответствует одной записи (строке). Значение по умолчанию – 0.
-   `kafka_commit_every_batch` — включает или отключает режим записи каждой принятой и обработанной пачки по отдельности вместо единой записи целого блока (по умолчанию `0`).
-   `kafka_thread_per_consumer` — включает или отключает предоставление отдельного потока каждому потребителю (по умолчанию `0`). При включенном режиме каждый потребитель сбрасывает данные независимо и параллельно, при отключённом — строки с данными от нескольких потребителей собираются в один блок.

Примеры

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

<summary>Устаревший способ создания таблицы</summary>

    :::note "Attention"
    Не используйте этот метод в новых проектах. По возможности переключите старые проекты на метод, описанный выше.

``` sql
Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
      [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_skip_broken_messages])
```
    :::
</details>

## Описание {#opisanie}

Полученные сообщения отслеживаются автоматически, поэтому из одной группы каждое сообщение считывается только один раз. Если необходимо получить данные дважды, то создайте копию таблицы с другим именем группы.

Группы пластичны и синхронизированы на кластере. Например, если есть 10 топиков и 5 копий таблицы в кластере, то в каждую копию попадет по 2 топика. Если количество копий изменится, то распределение топиков по копиям изменится автоматически. Подробно читайте об этом на http://kafka.apache.org/intro.

Чтение сообщения с помощью `SELECT` не слишком полезно (разве что для отладки), поскольку каждое сообщения может быть прочитано только один раз. Практичнее создавать потоки реального времени с помощью материализованных преставлений. Для этого:

1.  Создайте потребителя Kafka с помощью движка и рассматривайте его как поток данных.
2.  Создайте таблицу с необходимой структурой.
3.  Создайте материализованное представление, которое преобразует данные от движка и помещает их в ранее созданную таблицу.

Когда к движку присоединяется материализованное представление (`MATERIALIZED VIEW`), оно начинает в фоновом режиме собирать данные. Это позволяет непрерывно получать сообщения от Kafka и преобразовывать их в необходимый формат с помощью `SELECT`.
Материализованных представлений у одной kafka таблицы может быть сколько угодно, они не считывают данные из таблицы kafka непосредственно, а получают новые записи (блоками), таким образом можно писать в несколько таблиц с разным уровнем детализации (с группировкой - агрегацией и без).

Пример:

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

Для улучшения производительности полученные сообщения группируются в блоки размера [max_insert_block_size](../../../operations/settings/settings.md#settings-max_insert_block_size). Если блок не удалось сформировать за [stream_flush_interval_ms](../../../operations/settings/settings.md#stream-flush-interval-ms) миллисекунд, то данные будут сброшены в таблицу независимо от полноты блока.

Чтобы остановить получение данных топика или изменить логику преобразования, отсоедините материализованное представление:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Если необходимо изменить целевую таблицу с помощью `ALTER`, то материализованное представление рекомендуется отключить, чтобы избежать несостыковки между целевой таблицей и данными от представления.

## Конфигурация {#konfiguratsiia}

Аналогично GraphiteMergeTree, движок Kafka поддерживает расширенную конфигурацию с помощью конфигурационного файла ClickHouse. Существует два конфигурационных ключа, которые можно использовать: глобальный (`kafka`) и по топикам (`kafka_topic_*`). Сначала применяется глобальная конфигурация, затем конфигурация по топикам (если она существует).

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

В документе [librdkafka configuration reference](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) можно увидеть список возможных опций конфигурации. Используйте подчеркивание (`_`) вместо точки в конфигурации ClickHouse. Например, `check.crcs=true` будет соответствовать `<check_crcs>true</check_crcs>`.

### Поддержка Kerberos {#kafka-kerberos-support}

Чтобы начать работу с Kafka с поддержкой Kerberos, добавьте дочерний элемент `security_protocol` со значением `sasl_plaintext`. Этого будет достаточно, если получен тикет на получение тикета (ticket-granting ticket) Kerberos и он кэшируется средствами ОС.
ClickHouse может поддерживать учетные данные Kerberos с помощью файла keytab. Рассмотрим дочерние элементы `sasl_kerberos_service_name`, `sasl_kerberos_keytab` и `sasl_kerberos_principal`.

Пример:

``` xml
  <!-- Kerberos-aware Kafka -->
  <kafka>
    <security_protocol>SASL_PLAINTEXT</security_protocol>
	<sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
	<sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
  </kafka>
```

## Виртуальные столбцы {#virtualnye-stolbtsy}

-   `_topic` — топик Kafka.
-   `_key` — ключ сообщения.
-   `_offset` — оффсет сообщения.
-   `_timestamp` — временная метка сообщения.
-   `_partition` — секция топика Kafka.

**Смотрите также**

-   [Виртуальные столбцы](index.md#table_engines-virtual_columns)
-   [background_message_broker_schedule_pool_size](../../../operations/settings/settings.md#background_message_broker_schedule_pool_size)

