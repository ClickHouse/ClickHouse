---
description: 'Этот движок позволяет интегрировать ClickHouse с RabbitMQ.'
sidebar_label: 'RabbitMQ'
sidebar_position: 170
slug: /engines/table-engines/integrations/rabbitmq
title: 'Движок таблицы RabbitMQ'
doc_type: 'guide'
---

Этот движок позволяет интегрировать ClickHouse с [RabbitMQ](https://www.rabbitmq.com).

`RabbitMQ` позволяет:

* Публиковать потоки данных и подписываться на них.
* Обрабатывать потоки по мере их поступления.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port' [or rabbitmq_address = 'amqp(s)://guest:guest@localhost/vhost'],
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_secure = 0,]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N,]
    [rabbitmq_queue_settings_list = 'x-dead-letter-exchange=my-dlx,x-max-length=10,x-overflow=reject-publish',]
    [rabbitmq_queue_consume = false,]
    [rabbitmq_address = '',]
    [rabbitmq_vhost = '/',]
    [rabbitmq_username = '',]
    [rabbitmq_password = '',]
    [rabbitmq_commit_on_select = false,]
    [rabbitmq_max_rows_per_message = 1,]
    [rabbitmq_handle_error_mode = 'default']
```

Обязательные параметры:

* `rabbitmq_host_port` – хост:порт (например, `localhost:5672`).
* `rabbitmq_exchange_name` – имя exchange в RabbitMQ.
* `rabbitmq_format` – формат сообщения. Используется та же нотация, что и в SQL-функции `FORMAT`, например `JSONEachRow`. Подробнее см. в разделе [Форматы](../../../interfaces/formats.md).

Необязательные параметры:

* `rabbitmq_exchange_type` – Тип exchange RabbitMQ: `direct`, `fanout`, `topic`, `headers`, `consistent_hash`. По умолчанию: `fanout`.
* `rabbitmq_routing_key_list` – Список ключей маршрутизации, разделенных запятыми.
* `rabbitmq_schema` – Параметр, который необходимо использовать, если формат требует определения схемы. Например, [Cap&#39;n Proto](https://capnproto.org/) требует указать путь к файлу схемы и имя корневого объекта `schema.capnp:Message`.
* `rabbitmq_num_consumers` – Количество consumers на таблицу. Укажите больше consumers, если пропускной способности одного consumer недостаточно. По умолчанию: `1`
* `rabbitmq_num_queues` – Общее количество очередей. Увеличение этого числа может значительно повысить производительность. По умолчанию: `1`.
* `rabbitmq_queue_base` - Укажите префикс для имен очередей. Варианты использования этого параметра описаны ниже.
* `rabbitmq_persistent` - Если установлено значение 1 (true), для запроса вставки режим доставки будет установлен в 2 (сообщения помечаются как `persistent`). По умолчанию: `0`.
* `rabbitmq_skip_broken_messages` – Допуск парсера сообщений RabbitMQ к сообщениям, несовместимым со схемой, на блок. Если `rabbitmq_skip_broken_messages = N`, то движок пропускает *N* сообщений RabbitMQ, которые не удается разобрать (одно сообщение соответствует одной строке данных). По умолчанию: `0`.
* `rabbitmq_max_block_size` - Количество строк, собираемых перед сбросом данных из RabbitMQ. По умолчанию: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `rabbitmq_flush_interval_ms` - Тайм-аут для сброса данных из RabbitMQ. По умолчанию: [stream&#95;flush&#95;interval&#95;ms](/ru/operations/settings/settings#stream_flush_interval_ms).
* `rabbitmq_queue_settings_list` - позволяет задавать параметры RabbitMQ при создании очереди. Доступные параметры: `x-max-length`, `x-max-length-bytes`, `x-message-ttl`, `x-expires`, `x-priority`, `x-max-priority`, `x-overflow`, `x-dead-letter-exchange`, `x-queue-type`. Параметр `durable` для очереди включается автоматически.
* `rabbitmq_address` - Адрес для подключения. Используйте либо этот параметр, либо `rabbitmq_host_port`.
* `rabbitmq_vhost` - vhost RabbitMQ. По умолчанию: `'/'`.
* `rabbitmq_queue_consume` - Использовать очереди, заданные пользователем, и не выполнять никакой настройки RabbitMQ: объявление exchanges, очередей, привязок. По умолчанию: `false`.
* `rabbitmq_username` - Имя пользователя RabbitMQ.
* `rabbitmq_password` - Пароль RabbitMQ.
* `reject_unhandled_messages` - Отклонять сообщения (отправлять отрицательное подтверждение RabbitMQ) в случае ошибок. Этот параметр автоматически включается, если в `rabbitmq_queue_settings_list` задан `x-dead-letter-exchange`.
* `rabbitmq_commit_on_select` - Выполнять коммит сообщений при выполнении запроса SELECT. По умолчанию: `false`.
* `rabbitmq_max_rows_per_message` — Максимальное количество строк, записываемых в одно сообщение RabbitMQ для построчных форматов. По умолчанию: `1`.
* `rabbitmq_empty_queue_backoff_start_ms` — Начальная точка задержки для повторного планирования чтения, если очередь RabbitMQ пуста.
* `rabbitmq_empty_queue_backoff_end_ms` — Конечная точка задержки для повторного планирования чтения, если очередь RabbitMQ пуста.
* `rabbitmq_empty_queue_backoff_step_ms` — Шаг задержки для повторного планирования чтения, если очередь RabbitMQ пуста.
* `rabbitmq_handle_error_mode` — Как обрабатывать ошибки в движке RabbitMQ. Возможные значения: default (если не удается разобрать сообщение, будет сгенерировано исключение), stream (сообщение об исключении и необработанное сообщение будут сохранены в виртуальных столбцах `_error` и `_raw_message`), dead&#95;letter&#95;queue (данные, связанные с ошибкой, будут сохранены в system.dead&#95;letter&#95;queue).

<div id="ssl-connection">
  ### SSL-соединение
</div>

Используйте либо `rabbitmq_secure = 1`, либо `amqps` в адресе подключения: `rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`.
Используемая библиотека по умолчанию не проверяет, насколько безопасно установленное TLS‑соединение. Независимо от того, истёк ли срок действия сертификата, является ли он самоподписанным, отсутствует или недействителен, соединение всё равно разрешается. В будущем может быть реализована более строгая проверка сертификатов.

Наряду с настройками RabbitMQ также можно добавить настройки формата.

Пример:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5,
                            date_time_input_format = 'best_effort';
```

Конфигурацию сервера RabbitMQ следует добавить в файл конфигурации ClickHouse.

Требуемая конфигурация:

```xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

Дополнительные настройки:

```xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

<div id="description">
  ## Описание
</div>

`SELECT` не слишком полезен для чтения сообщений (кроме отладки), поскольку каждое сообщение можно прочитать только один раз. Гораздо практичнее создавать потоки в реальном времени с помощью [materialized views](../../../sql-reference/statements/create/view.md). Для этого:

1. Используйте движок, чтобы создать consumer RabbitMQ, и рассматривайте его как поток данных.
2. Создайте таблицу с нужной структурой.
3. Создайте materialized view, которое преобразует данные из движка и помещает их в ранее созданную таблицу.

Когда `MATERIALIZED VIEW` подключается к движку, оно начинает собирать данные в фоновом режиме. Это позволяет непрерывно получать сообщения из RabbitMQ и преобразовывать их в нужный формат с помощью `SELECT`.
Одна таблица RabbitMQ может иметь сколько угодно materialized views.

Данные могут маршрутизироваться на основе `rabbitmq_exchange_type` и указанного `rabbitmq_routing_key_list`.
Для одной таблицы можно указать не более одного exchange. Один exchange может использоваться несколькими таблицами — это позволяет одновременно маршрутизировать данные в несколько таблиц.

Варианты типа exchange:

* `direct` - Маршрутизация основана на точном совпадении ключей. Example списка ключей таблицы: `key1,key2,key3,key4,key5`, ключ сообщения может совпадать с любым из них.
* `fanout` - Маршрутизация во все таблицы (где имя exchange одинаково) независимо от ключей.
* `topic` - Маршрутизация основана на шаблонах с ключами, разделенными точками. Examples: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
* `headers` - Маршрутизация основана на совпадениях `key=value` с настройкой `x-match=all` или `x-match=any`. Example списка ключей таблицы: `x-match=all,format=logs,type=report,year=2020`.
* `consistent_hash` - Данные равномерно распределяются между всеми привязанными таблицами (где имя exchange одинаково). Обратите внимание, что этот тип exchange должен быть включен с помощью plugin RabbitMQ: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

Настройка `rabbitmq_queue_base` может использоваться в следующих случаях:

* чтобы разные таблицы могли совместно использовать очереди, а для одних и тех же очередей можно было зарегистрировать несколько consumers, что повышает производительность. При использовании настроек `rabbitmq_num_consumers` и/или `rabbitmq_num_queues` точное совпадение очередей достигается, если эти параметры одинаковы.
* чтобы можно было восстановить чтение из определенных долговечных очередей, если не все сообщения были успешно обработаны. Чтобы возобновить consumption из одной конкретной очереди, задайте ее имя в настройке `rabbitmq_queue_base` и не указывайте `rabbitmq_num_consumers` и `rabbitmq_num_queues` (по умолчанию равно 1). Чтобы возобновить consumption из всех очередей, объявленных для конкретной таблицы, просто укажите те же настройки: `rabbitmq_queue_base`, `rabbitmq_num_consumers`, `rabbitmq_num_queues`. По умолчанию имена очередей будут уникальны для таблиц.
* чтобы повторно использовать очереди, так как они объявлены как durable и не удаляются автоматически. (Их можно удалить с помощью любого из CLI-инструментов RabbitMQ.)

Для повышения производительности полученные сообщения группируются в blocks размером [max&#95;insert&#95;block&#95;size](/ru/operations/settings/settings#max_insert_block_size). Если block не был сформирован в течение [stream&#95;flush&#95;interval&#95;ms](../../../operations/server-configuration-parameters/settings.md) миллисекунд, данные будут сброшены в таблицу независимо от полноты block.

Если настройки `rabbitmq_num_consumers` и/или `rabbitmq_num_queues` указаны вместе с `rabbitmq_exchange_type`, тогда:

* plugin `rabbitmq-consistent-hash-exchange` должен быть включен.
* должно быть указано свойство `message_id` у публикуемых сообщений (уникальное для каждого сообщения/батча).

Для запроса вставки доступны метаданные сообщения, которые добавляются к каждому опубликованному сообщению: `messageID` и флаг `republished` (true, если сообщение было опубликовано более одного раза) — к ним можно получить доступ через headers сообщения.

Не используйте одну и ту же таблицу для вставок и materialized views.

Example:

```sql
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
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_exchange_name` - имя exchange в RabbitMQ. Тип данных: `String`.
* `_channel_id` - ChannelID канала, в котором был объявлен consumer, получивший сообщение. Тип данных: `String`.
* `_delivery_tag` - DeliveryTag полученного сообщения. Уникален в пределах канала. Тип данных: `UInt64`.
* `_redelivered` - флаг `redelivered` сообщения. Тип данных: `UInt8`.
* `_message_id` - messageID полученного сообщения; непустой, если был задан при публикации сообщения. Тип данных: `String`.
* `_timestamp` - временная метка полученного сообщения; непустая, если была задана при публикации сообщения. Тип данных: `UInt64`.

Дополнительные виртуальные столбцы, когда `rabbitmq_handle_error_mode='stream'`:

* `_raw_message` - необработанное сообщение, которое не удалось успешно разобрать. Тип данных: `Nullable(String)`.
* `_error` - сообщение об исключении, возникшем при неудачном разборе. Тип данных: `Nullable(String)`.

Примечание: виртуальные столбцы `_raw_message` и `_error` заполняются только в случае исключения при разборе; если сообщение успешно разобрано, они всегда имеют значение `NULL`.

<div id="caveats">
  ## Ограничения
</div>

Хотя в определении таблицы можно указать [выражения столбцов по умолчанию](/ru/sql-reference/statements/create/table.md/#default_values) (например, `DEFAULT`, `MATERIALIZED`, `ALIAS`), они будут проигнорированы. Вместо этого столбцы будут заполнены соответствующими значениями по умолчанию для их типов.

<div id="data-formats-support">
  ## Поддержка форматов данных
</div>

Движок RabbitMQ поддерживает все [форматы](../../../interfaces/formats.md), поддерживаемые в ClickHouse.
Количество строк в одном сообщении RabbitMQ зависит от того, является ли формат построчным или блочным:

* Для построчных форматов количество строк в одном сообщении RabbitMQ можно настроить с помощью параметра `rabbitmq_max_rows_per_message`.
* Для блочных форматов мы не можем разделить блок на более мелкие части, однако количество строк в одном блоке можно задать общей настройкой [max&#95;block&#95;size](/ru/operations/settings/settings#max_block_size).