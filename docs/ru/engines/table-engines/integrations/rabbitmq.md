# RabbitMQ {#rabbitmq-engine}

Движок работает с [RabbitMQ](https://www.rabbitmq.com).

`RabbitMQ` позволяет:

-   Публиковать/подписываться на потоки данных.
-   Обрабатывать потоки по мере их появления.

## Создание таблицы {#table_engine-rabbitmq-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port',
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_row_delimiter = 'delimiter_symbol',]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N]
```

Обязательные параметры:

-   `rabbitmq_host_port` – адрес сервера (`хост:порт`). Например: `localhost:5672`.
-   `rabbitmq_exchange_name` – имя точки обмена в RabbitMQ.
-   `rabbitmq_format` – формат сообщения. Используется такое же обозначение, как и в функции `FORMAT` в SQL, например, `JSONEachRow`. Подробнее см. в разделе [Форматы входных и выходных данных](../../../interfaces/formats.md).

Дополнительные параметры:

-   `rabbitmq_exchange_type` – тип точки обмена в RabbitMQ: `direct`, `fanout`, `topic`, `headers`, `consistent_hash`. По умолчанию: `fanout`.
-   `rabbitmq_routing_key_list` – список ключей маршрутизации, через запятую.
-   `rabbitmq_row_delimiter` – символ-разделитель, который завершает сообщение.
-   `rabbitmq_schema` – опциональный параметр, необходимый, если используется формат, требующий определения схемы. Например, [Cap’n Proto](https://capnproto.org/) требует путь к файлу со схемой и название корневого объекта `schema.capnp:Message`.
-   `rabbitmq_num_consumers` – количество потребителей на таблицу. По умолчанию: `1`. Укажите больше потребителей, если пропускная способность одного потребителя недостаточна.
-   `rabbitmq_num_queues` – количество очередей. По умолчанию: `1`. Большее число очередей может сильно увеличить пропускную способность.
-   `rabbitmq_queue_base` - настройка для имен очередей. Сценарии использования описаны ниже.
-   `rabbitmq_persistent` - флаг, от которого зависит настройка 'durable' для сообщений при запросах `INSERT`. По умолчанию: `0`.
-   `rabbitmq_skip_broken_messages` – максимальное количество некорректных сообщений в блоке. Если `rabbitmq_skip_broken_messages = N`, то движок отбрасывает `N` сообщений, которые не получилось обработать. Одно сообщение в точности соответствует одной записи (строке). Значение по умолчанию – 0.
-   `rabbitmq_max_block_size`
-   `rabbitmq_flush_interval_ms`

Требуемая конфигурация:

Конфигурация сервера RabbitMQ добавляется с помощью конфигурационного файла ClickHouse.

``` xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

Example:

``` sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;
```

## Описание {#description}

Запрос `SELECT` не очень полезен для чтения сообщений (за исключением отладки), поскольку каждое сообщение может быть прочитано только один раз. Практичнее создавать потоки реального времени с помощью [материализованных преставлений](../../../sql-reference/statements/create/view.md). Для этого:

1.  Создайте потребителя RabbitMQ с помощью движка и рассматривайте его как поток данных.
2.  Создайте таблицу с необходимой структурой.
3.  Создайте материализованное представление, которое преобразует данные от движка и помещает их в ранее созданную таблицу.

Когда к движку присоединяется материализованное представление, оно начинает в фоновом режиме собирать данные. Это позволяет непрерывно получать сообщения от RabbitMQ и преобразовывать их в необходимый формат с помощью `SELECT`.
У одной таблицы RabbitMQ может быть неограниченное количество материализованных представлений.

Данные передаются с помощью параметров `rabbitmq_exchange_type` и `rabbitmq_routing_key_list`.
Может быть не более одной точки обмена на таблицу. Одна точка обмена может использоваться несколькими таблицами: это позволяет выполнять маршрутизацию по нескольким таблицам одновременно.

Параметры точек обмена:

-   `direct` - маршрутизация основана на точном совпадении ключей. Пример списка ключей: `key1,key2,key3,key4,key5`. Ключ сообщения может совпадать с одним из них.
-   `fanout` - маршрутизация по всем таблицам, где имя точки обмена совпадает, независимо от ключей.
-   `topic` - маршрутизация основана на правилах с ключами, разделенными точками. Например: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
-   `headers` - маршрутизация основана на совпадении `key=value` с настройкой `x-match=all` или `x-match=any`. Пример списка ключей таблицы: `x-match=all,format=logs,type=report,year=2020`.
-   `consistent_hash` - данные равномерно распределяются между всеми связанными таблицами, где имя точки обмена совпадает. Обратите внимание, что этот тип обмена должен быть включен с помощью плагина RabbitMQ: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

Настройка `rabbitmq_queue_base` может быть использована в следующих случаях:
1.   чтобы восстановить чтение из ранее созданных очередей, если оно прекратилось по какой-либо причине, но очереди остались непустыми. Для восстановления чтения из одной конкретной очереди, нужно написать ее имя в `rabbitmq_queue_base` настройку и не указывать настройки `rabbitmq_num_consumers` и `rabbitmq_num_queues`. Чтобы восстановить чтение из всех очередей, которые были созданы для конкретной таблицы, необходимо совпадение следующих настроек: `rabbitmq_queue_base`, `rabbitmq_num_consumers`, `rabbitmq_num_queues`. По умолчанию, если настройка `rabbitmq_queue_base` не указана, будут использованы уникальные для каждой таблицы имена очередей.
2.   чтобы объявить одни и те же очереди для разных таблиц, что позволяет создавать несколько параллельных подписчиков на каждую из очередей. То есть обеспечивается лучшая производительность. В данном случае, для таких таблиц также необходимо совпадение настроек: `rabbitmq_num_consumers`, `rabbitmq_num_queues`.
3.   чтобы повторно использовать созданные c `durable` настройкой очереди, так как они не удаляются автоматически (но могут быть удалены с помощью любого RabbitMQ CLI).

Для улучшения производительности полученные сообщения группируются в блоки размера [max_insert_block_size](../../../operations/settings/settings.md#settings-max_insert_block_size). Если блок не удалось сформировать за [stream_flush_interval_ms](../../../operations/settings/settings.md#stream-flush-interval-ms) миллисекунд, то данные будут сброшены в таблицу независимо от полноты блока.

Если параметры`rabbitmq_num_consumers` и/или `rabbitmq_num_queues` заданы вместе с параметром `rabbitmq_exchange_type`:

-   плагин `rabbitmq-consistent-hash-exchange` должен быть включен.
-   свойство `message_id` должно быть определено (уникальное для каждого сообщения/пакета).

При запросах `INSERT` отправляемым сообщениям добавляются метаданные: `messageID` и флаг `republished` - доступны через заголовки сообщений (headers).
Для запросов чтения и вставки не должна использоваться одна и та же таблица.

Пример:

``` sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_exchange_type = 'headers',
                            rabbitmq_routing_key_list = 'format=logs,type=report,year=2020',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree();

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

## Virtual Columns {#virtual-columns}

-   `_exchange_name` - имя точки обмена RabbitMQ.
-   `_channel_id` - идентификатор канала `ChannelID`, на котором было получено сообщение.
-   `_delivery_tag` - значение `DeliveryTag` полученного сообщения. Уникально в рамках одного канала.
-   `_redelivered` - флаг `redelivered`. (Не равно нулю, если есть возможность, что сообщение было получено более, чем одним каналом.)
-   `_message_id` - значение поля `messageID` полученного сообщения. Данное поле непусто, если указано в параметрах при отправке сообщения.
-   `_timestamp` - значение поля `timestamp` полученного сообщения. Данное поле непусто, если указано в параметрах при отправке сообщения.
