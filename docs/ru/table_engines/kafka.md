# Kafka

Движок работает с [Apache Kafka](http://kafka.apache.org/). 

Kafka позволяет:

- Публиковать/подписываться на потоки данных.
- Организовать отказо-устойчивое хранилище.
- Обрабатывать потоки по мере их появления.

```
Kafka(broker_list, topic_list, group_name, format[, schema])
```

Параметры:
- `broker_list` - Перечень брокеров, разделенный запятыми (`localhost:9092`).
- `topic_list` - Перечень топиков Kafka(``my_topic``).
- `group_name` - Группа потребителя Kafka (``group1``). Отступы для чтения отслеживаются для каждой группы отдельно.
- `format` - Формат сообщений. Имеет те же обозначения, что выдает SQL-выражение `FORMAT`, например, `JSONEachRow`.
- `schema` - Опциональный параметр, необходимый, если используется формат, требующий определения схемы. Например, [Cap'n Proto](https://capnproto.org/) требует путь к файлу со схемой и название корневого объекта `schema.capnp:Message`.

Пример:

```sql
CREATE TABLE queue (timestamp UInt64, level String, message String) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');
SELECT * FROM queue LIMIT 5
```

Полученные сообщения отслеживаются автоматически, поэтому из одной группы каждое сообщение считывается только один раз. Если необходимо получить данные дважды, то создайте копию таблицы с другим именем группы.

Группы пластичны и синхронизированы на кластере. Например, если есть 10 топиков и 5 копий таблицы в кластере, то в каждую копию попадет по 2 топика. Если количество копий изменится, то распределение топиков по копиям изменится автоматически. Подробно читайте об этом на [http://kafka.apache.org/intro](http://kafka.apache.org/intro).

Kafka обычно используется для построения систем работающих в режиме реального времени. Если к движку присоединить материализованные представления (`MATERIALIZED VIEW`), то он будет в фоновом режиме собирать данные и раскладывать их в соответствующие представления. Что позволяет непрерывно получать сообщения от Kafka и преобразовывать их в необходимый формат с помощью `SELECT`.

Пример:

```sql
CREATE TABLE queue (timestamp UInt64, level String, message String) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

CREATE MATERIALIZED VIEW daily ENGINE = SummingMergeTree(day, (day, level), 8192) AS
SELECT toDate(toDateTime(timestamp)) AS day, level, count() as total
FROM queue GROUP BY day, level;

SELECT level, sum(total) FROM daily GROUP BY level;
```

Для улучшения производительности полученные сообщения группируются в блоки размера [max_insert_block_size](../../operations/settings/settings.html#settings-settings-max_insert_block_size). Если блок не удалось сформировать за [stream_flush_interval_ms](../../operations/settings/settings.html#settings-settings_stream_flush_interval_ms) миллисекунд, то данные будут сброшены в таблицу независимо от полноты блока.