---
toc_priority: 6
toc_title: RabbitMQ
---

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
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_transactional_channel = 0]
```

Обязательные параметры:

-   `rabbitmq_host_port` – адрес сервера (`хост:порт`). Например: `localhost:5672`.
-   `rabbitmq_exchange_name` – имя точки обмена в RabbitMQ.
-   `rabbitmq_format` – формат сообщения. Используется такое же обозначение, как и в функции `FORMAT` в SQL, например, `JSONEachRow`. Подробнее см. в разделе [Форматы входных и выходных данных](../../../interfaces/formats.md).

Дополнительные параметры:

-   `rabbitmq_exchange_type` – тип точки обмена в RabbitMQ: `direct`, `fanout`, `topic`, `headers`, `consistent-hash`. По умолчанию: `fanout`.
-   `rabbitmq_routing_key_list` – список ключей маршрутизации, через запятую.
-   `rabbitmq_row_delimiter` – символ-разделитель, который завершает сообщение.
-   `rabbitmq_num_consumers` – количество потребителей на таблицу. По умолчанию: `1`. Укажите больше потребителей, если пропускная способность одного потребителя недостаточна.
-   `rabbitmq_num_queues` – количество очередей на потребителя. По умолчанию: `1`. Укажите больше потребителей, если пропускная способность одной очереди на потребителя недостаточна. Одна очередь поддерживает до 50 тысяч сообщений одновременно.
-   `rabbitmq_transactional_channel` – обернутые запросы `INSERT` в  транзакциях. По умолчанию: `0`.

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
-   `consistent-hash` - данные равномерно распределяются между всеми связанными таблицами, где имя точки обмена совпадает. Обратите внимание, что этот тип обмена должен быть включен с помощью плагина RabbitMQ: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

Если тип точки обмена не задан, по умолчанию используется `fanout`.  В таком случае ключи маршрутизации для публикации данных должны быть рандомизированы в диапазоне `[1, num_consumers]` за каждое сообщение/пакет (или в диапазоне `[1, num_consumers * num_queues]`, если `rabbitmq_num_queues` задано). Эта конфигурация таблицы работает быстрее, чем любая другая, особенно когда заданы параметры  `rabbitmq_num_consumers` и/или `rabbitmq_num_queues`.

Если параметры`rabbitmq_num_consumers` и/или `rabbitmq_num_queues` заданы вместе с параметром `rabbitmq_exchange_type`:

-   плагин `rabbitmq-consistent-hash-exchange` должен быть включен.
-   свойство `message_id` должно быть определено (уникальное для каждого сообщения/пакета).

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
