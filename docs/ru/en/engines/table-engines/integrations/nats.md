---
description: 'Этот движок позволяет интегрировать ClickHouse с NATS, чтобы публиковать
  сообщения в subjects или подписываться на них, а также обрабатывать новые сообщения
  по мере их поступления.'
sidebar_label: 'NATS'
sidebar_position: 140
slug: /engines/table-engines/integrations/nats
title: 'Движок таблицы NATS'
doc_type: 'guide'
---

Этот движок позволяет интегрировать ClickHouse с [NATS](https://nats.io/).

`NATS` позволяет:

* Публиковать сообщения в subjects или подписываться на них.
* Обрабатывать новые сообщения по мере их поступления.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = NATS SETTINGS
    nats_url = 'host:port',
    nats_subjects = 'subject1,subject2,...',
    nats_format = 'data_format'[,]
    [nats_schema = '',]
    [nats_num_consumers = N,]
    [nats_queue_group = 'group_name',]
    [nats_secure = false,]
    [nats_max_reconnect = N,]
    [nats_reconnect_wait = N,]
    [nats_server_list = 'host1:port1,host2:port2,...',]
    [nats_skip_broken_messages = N,]
    [nats_max_block_size = N,]
    [nats_flush_interval_ms = N,]
    [nats_username = 'user',]
    [nats_password = 'password',]
    [nats_token = 'clickhouse',]
    [nats_credential_file = '/var/nats_credentials',]
    [nats_startup_connect_tries = 5,]
    [nats_max_rows_per_message = 1,]
    [nats_handle_error_mode = 'default']
```

Обязательные параметры:

* `nats_url` – хост:порт (например, `localhost:4222`).
* `nats_subjects` – Список subject, на которые таблица NATS будет подписываться или в которые будет публиковать сообщения. Поддерживаются subject с подстановочными знаками, например `foo.*.bar` или `baz.>`
* `nats_format` – Формат сообщения. Использует ту же нотацию, что и SQL-функция `FORMAT`, например `JSONEachRow`. Дополнительные сведения см. в разделе [Форматы](../../../interfaces/formats.md).

Необязательные параметры:

* `nats_schema` – Параметр, который необходимо использовать, если формат требует определения схемы. Например, [Cap&#39;n Proto](https://capnproto.org/) требует указать путь к файлу схемы и имя корневого объекта `schema.capnp:Message`.
* `nats_stream` – Имя существующего stream в NATS JetStream.
* `nats_consumer_name` – Имя существующего durable pull consumer в NATS JetStream.
* `nats_num_consumers` – Количество consumers на таблицу. Значение по умолчанию: `1`. Укажите больше consumers, если пропускной способности одного consumer недостаточно только для NATS core.
* `nats_queue_group` – Имя queue group для подписчиков NATS. По умолчанию используется имя таблицы.
* `nats_max_reconnect` – Устарел и не имеет эффекта; переподключение выполняется постоянно с тайм-аутом `nats_reconnect_wait`.
* `nats_reconnect_wait` – Время ожидания в миллисекундах между попытками переподключения. Значение по умолчанию: `2000`.
* `nats_server_list` - Список серверов для подключения. Можно указать для подключения к cluster NATS.
* `nats_skip_broken_messages` - Допустимое для parser NATS количество несовместимых со схемой сообщений на block. Значение по умолчанию: `0`. Если `nats_skip_broken_messages = N`, то движок пропускает *N* сообщений NATS, которые не удаётся разобрать (одно сообщение соответствует одной строке данных).
* `nats_max_block_size` - Количество строк, собираемых при poll(s) для сброса данных из NATS. Значение по умолчанию: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `nats_flush_interval_ms` - Тайм-аут сброса данных, прочитанных из NATS. Значение по умолчанию: [stream&#95;flush&#95;interval&#95;ms](/ru/operations/settings/settings#stream_flush_interval_ms).
* `nats_username` - Имя пользователя NATS.
* `nats_password` - Пароль NATS.
* `nats_token` - Токен аутентификации NATS.
* `nats_credential_file` - Путь к файлу учетных данных NATS.
* `nats_startup_connect_tries` - Количество попыток подключения при запуске. Значение по умолчанию: `5`.
* `nats_max_rows_per_message` — Максимальное количество строк, записываемых в одном сообщении NATS для построчных форматов. (по умолчанию: `1`).
* `nats_handle_error_mode` — Как обрабатывать ошибки в движке NATS. Возможные значения: default (если не удаётся разобрать сообщение, будет сгенерировано исключение), stream (текст исключения и необработанное сообщение будут сохранены в виртуальных столбцах `_error` и `_raw_message`).

SSL-подключение:

Для безопасного подключения используйте `nats_secure = 1`.
Проверка сертификата управляется переменной окружения `CLICKHOUSE_NATS_TLS_SECURE`;
Если сертификат просрочен, самоподписан, отсутствует или по иной причине недействителен, отключите проверку, установив `CLICKHOUSE_NATS_TLS_SECURE=0`.

Запись в таблицу NATS:

Если таблица читает только из одного subject, любая вставка будет опубликована в тот же subject.
Однако если таблица читает из нескольких subjects, нужно указать, в какой именно subject следует публиковать данные.
Именно поэтому при вставке в таблицу с несколькими subjects необходимо задать `stream_like_engine_insert_queue`.
Вы можете выбрать один из subjects, из которых читает таблица, и опубликовать свои данные туда. Например:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1,subject2',
             nats_format = 'JSONEachRow';

  INSERT INTO queue
  SETTINGS stream_like_engine_insert_queue = 'subject2'
  VALUES (1, 1);
```

Также можно добавить настройки формата наряду с настройками, связанными с NATS.

Пример:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';
```

Конфигурацию сервера NATS можно добавить с помощью файла конфигурации ClickHouse.
В частности, можно добавить пароль для движка NATS:

```xml
<nats>
    <user>click</user>
    <password>house</password>
    <token>clickhouse</token>
</nats>
```

<div id="description">
  ## Описание
</div>

`SELECT` не слишком полезен для чтения сообщений (кроме отладки), поскольку каждое сообщение можно прочитать только один раз. Гораздо практичнее создавать потоки в реальном времени с помощью [materialized views](../../../sql-reference/statements/create/view.md). Для этого:

1. Используйте движок, чтобы создать consumer NATS, и рассматривайте его как поток данных.
2. Создайте таблицу с нужной структурой.
3. Создайте materialized view, которое преобразует данные из движка и помещает их в ранее созданную таблицу.

Когда `MATERIALIZED VIEW` подключается к движку, оно начинает собирать данные в фоновом режиме. Это позволяет непрерывно получать сообщения из NATS и преобразовывать их в нужный формат с помощью `SELECT`.
У одной таблицы NATS может быть сколько угодно materialized views; они не читают данные из таблицы напрямую, а получают новые записи (блоками), поэтому вы можете записывать данные в несколько таблиц с разным уровнем детализации (с группировкой — aggregation и без неё).

Пример:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

Чтобы прекратить получение данных из потоков или изменить логику преобразования, отключите materialized view:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Если вы хотите изменить целевую таблицу командой `ALTER`, мы рекомендуем отключить materialized view, чтобы избежать расхождений между целевой таблицей и данными представления.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_subject` - subject сообщения NATS. Тип данных: `String`.

Дополнительные виртуальные столбцы при `nats_handle_error_mode='stream'`:

* `_raw_message` - Исходное сообщение, которое не удалось успешно разобрать. Тип данных: `Nullable(String)`.
* `_error` - Сообщение об исключении, возникшем при неудачном разборе. Тип данных: `Nullable(String)`.

Примечание: виртуальные столбцы `_raw_message` и `_error` заполняются только в случае исключения при разборе; если сообщение успешно разобрано, они всегда имеют значение `NULL`.

<div id="data-formats-support">
  ## Поддержка форматов данных
</div>

Движок NATS поддерживает все [форматы](../../../interfaces/formats.md), поддерживаемые в ClickHouse.
Количество строк в одном сообщении NATS зависит от того, построчный формат или блочный:

* Для построчных форматов количество строк в одном сообщении NATS можно регулировать с помощью настройки `nats_max_rows_per_message`.
* Для блочных форматов блок нельзя разделить на более мелкие части, но количество строк в одном блоке можно регулировать с помощью общей настройки [max&#95;block&#95;size](/ru/operations/settings/settings#max_block_size).

<div id="using-jetstream">
  ## Использование JetStream
</div>

Перед использованием движка NATS с NATS JetStream необходимо создать stream NATS и durable pull consumer. Для этого можно использовать, например, утилиту `nats` из пакета [NATS CLI](https://github.com/nats-io/natscli):

<details>
  <summary>создание stream</summary>

  ```bash
  $ nats stream add
  ? Stream Name stream_name
  ? Subjects stream_subject
  ? Storage file
  ? Replication 1
  ? Retention Policy Limits
  ? Discard Policy Old
  ? Stream Messages Limit -1
  ? Per Subject Messages Limit -1
  ? Total Stream Size -1
  ? Message TTL -1
  ? Max Message Size -1
  ? Duplicate tracking time window 2m0s
  ? Allow message Roll-ups No
  ? Allow message deletion Yes
  ? Allow purging subjects or the entire stream Yes
  Stream stream_name was created

  Information for Stream stream_name created 2025-10-03 14:12:51

                  Subjects: stream_subject
                  Replicas: 1
                   Storage: File

  Options:

                 Retention: Limits
           Acknowledgments: true
            Discard Policy: Old
          Duplicate Window: 2m0s
                Direct Get: true
         Allows Msg Delete: true
              Allows Purge: true
    Allows Per-Message TTL: false
            Allows Rollups: false

  Limits:

          Maximum Messages: unlimited
       Maximum Per Subject: unlimited
             Maximum Bytes: unlimited
               Maximum Age: unlimited
      Maximum Message Size: unlimited
         Maximum Consumers: unlimited

  State:

                  Messages: 0
                     Bytes: 0 B
            First Sequence: 0
             Last Sequence: 0
          Active Consumers: 0
  ```
</details>

<details>
  <summary>создание durable pull consumer</summary>

  ```bash
  $ nats consumer add
  ? Select a Stream stream_name
  ? Consumer name consumer_name
  ? Delivery target (empty for Pull Consumers) 
  ? Start policy (all, new, last, subject, 1h, msg sequence) all
  ? Acknowledgment policy explicit
  ? Replay policy instant
  ? Filter Stream by subjects (blank for all) 
  ? Maximum Allowed Deliveries -1
  ? Maximum Acknowledgments Pending 0
  ? Deliver headers only without bodies No
  ? Add a Retry Backoff Policy No
  Information for Consumer stream_name > consumer_name created 2025-10-03T14:13:51+03:00

  Configuration:

                      Name: consumer_name
                 Pull Mode: true
            Deliver Policy: All
                Ack Policy: Explicit
                  Ack Wait: 30.00s
             Replay Policy: Instant
           Max Ack Pending: 1,000
         Max Waiting Pulls: 512

  State:

    Last Delivered Message: Consumer sequence: 0 Stream sequence: 0
      Acknowledgment Floor: Consumer sequence: 0 Stream sequence: 0
          Outstanding Acks: 0 out of maximum 1,000
      Redelivered Messages: 0
      Unprocessed Messages: 0
             Waiting Pulls: 0 of maximum 512
  ```
</details>

После создания stream и durable pull consumer можно создать таблицу с движком NATS. Для этого необходимо указать: nats&#95;stream, nats&#95;consumer&#95;name и nats&#95;subjects:

```SQL
CREATE TABLE nats_jet_stream (
    key UInt64,
    value UInt64
  ) ENGINE NATS 
    SETTINGS  nats_url = 'localhost:4222',
              nats_stream = 'stream_name',
              nats_consumer_name = 'consumer_name',
              nats_subjects = 'stream_subject',
              nats_format = 'JSONEachRow';
```