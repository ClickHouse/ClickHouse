---
description: 'Этот движок обеспечивает интеграцию с Azure Blob Storage,
  позволяя импортировать потоковые данные.'
sidebar_label: 'AzureQueue'
sidebar_position: 181
slug: /engines/table-engines/integrations/azure-queue
title: 'движок таблицы AzureQueue'
doc_type: 'reference'
---

Этот движок обеспечивает интеграцию с [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs), позволяя импортировать потоковые данные.

<div id="creating-a-table">
  ## Создать таблицу
</div>

```sql
CREATE TABLE test (name String, value UInt32)
    ENGINE = AzureQueue(...)
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    ...
```

**Параметры движка**

Параметры `AzureQueue` такие же, как у движка таблицы `AzureBlobStorage`. См. раздел с параметрами [здесь](../../../engines/table-engines/integrations/azureBlobStorage.md).

Как и в случае с движком таблицы [AzureBlobStorage](/ru/engines/table-engines/integrations/azureBlobStorage), пользователи могут использовать эмулятор Azurite для локальной разработки с Azure Storage. Подробности [здесь](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage).

**Пример**

```sql
CREATE TABLE azure_queue_engine_table
(
    `key` UInt64,
    `data` String
)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
SETTINGS mode = 'unordered'
```

<div id="settings">
  ## Настройки
</div>

Набор поддерживаемых настроек в целом такой же, как у движка таблицы `S3Queue`, но без префикса `s3queue_`. См. [полный список настроек](../../../engines/table-engines/integrations/s3queue.md#settings).
Чтобы получить список настроек, заданных для таблицы, используйте таблицу `system.azure_queue_settings`. Доступно с версии `24.10`.

Ниже приведены настройки, которые поддерживаются только в AzureQueue и не применяются к S3Queue.

<div id="after_processing_move_connection_string">
  ### `after_processing_move_connection_string`
</div>

Строка подключения к Azure Blob Storage, в которое будут перемещаться успешно обработанные файлы, если пункт назначения — другой контейнер Azure.

Возможные значения:

* String.

Значение по умолчанию: пустая строка.

<div id="after_processing_move_container">
  ### `after_processing_move_container`
</div>

Имя контейнера, в который будут перемещаться успешно обработанные файлы, если пункт назначения — другой контейнер Azure.

Возможные значения:

* String.

Значение по умолчанию: пустая строка.

Пример:

```sql
CREATE TABLE azure_queue_engine_table
(
    `key` UInt64,
    `data` String
)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
SETTINGS
    mode = 'unordered',
    after_processing = 'move',
    after_processing_move_connection_string = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    after_processing_move_container = 'dst-container';
```

<div id="select">
  ## SELECT из движка таблицы AzureQueue
</div>

Запросы SELECT для таблиц AzureQueue по умолчанию запрещены. Это соответствует распространённому паттерну очереди, при котором данные считываются один раз, а затем удаляются из очереди. SELECT запрещён, чтобы предотвратить случайную потерю данных.
Однако в некоторых случаях это может быть полезно. Для этого нужно установить для настройки `stream_like_engine_allow_direct_select` значение `True`.
У движка AzureQueue есть специальная настройка для запросов SELECT: `commit_on_select`. Установите её в `False`, чтобы сохранить данные в очереди после чтения, или в `True`, чтобы удалить их.

<div id="description">
  ## Описание
</div>

`SELECT` не особенно полезен для потокового импорта (кроме отладки), поскольку каждый файл можно импортировать только один раз. Гораздо практичнее создавать потоки в реальном времени с помощью [materialized views](../../../sql-reference/statements/create/view.md). Для этого:

1. С помощью движка создайте таблицу для чтения из указанного пути в Azure Blob Storage и рассматривайте её как поток данных.
2. Создайте таблицу с нужной структурой.
3. Создайте materialized view, которое преобразует данные из движка и помещает их в созданную ранее таблицу.

После присоединения `MATERIALIZED VIEW` к движку он начинает собирать данные в фоновом режиме.

Аргументы движка имеют вид `AzureQueue(connection_string, container_name, blobpath, format[, compression])`.

Пример:

```sql
CREATE TABLE azure_queue_engine_table (key UInt64, data String)
  ENGINE=AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
  SETTINGS
      mode = 'unordered';

CREATE TABLE stats (key UInt64, data String)
  ENGINE = MergeTree() ORDER BY key;

CREATE MATERIALIZED VIEW consumer TO stats
  AS SELECT key, data FROM azure_queue_engine_table;

SELECT * FROM stats ORDER BY key;
```

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — путь к файлу.
* `_file` — имя файла.

Подробнее о виртуальных столбцах см. [здесь](../../../engines/table-engines/index.md#table_engines-virtual_columns).

<div id="introspection">
  ## Интроспекция
</div>

Включите логирование для таблицы с помощью настройки таблицы `enable_logging_to_queue_log=1`.

Возможности интроспекции такие же, как у [движка таблицы S3Queue](/ru/engines/table-engines/integrations/s3queue#introspection), но есть несколько отличий:

1. Используйте `system.azure_queue_metadata_cache` для состояния очереди в памяти в версиях сервера &gt;= 25.1. Для более старых версий используйте `system.s3queue_metadata_cache` (он также содержит информацию для таблиц `azure`).
2. Включите `system.azure_queue_log` через основную конфигурацию ClickHouse, например.

```xml
  <azure_queue_log>
    <database>system</database>
    <table>azure_queue_log</table>
  </azure_queue_log>
```

Эта постоянная таблица содержит ту же информацию, что и `system.s3queue_metadata_cache`, но для обработанных файлов и файлов, обработка которых завершилась ошибкой.

Таблица имеет следующую структуру:

```sql

CREATE TABLE system.azure_queue_log
(
    `hostname` LowCardinality(String) COMMENT 'Hostname',
    `event_date` Date COMMENT 'Event date of writing this log row',
    `event_time` DateTime COMMENT 'Event time of writing this log row',
    `database` String COMMENT 'The name of a database where current S3Queue table lives.',
    `table` String COMMENT 'The name of S3Queue table.',
    `uuid` String COMMENT 'The UUID of S3Queue table',
    `file_name` String COMMENT 'File name of the processing file',
    `rows_processed` UInt64 COMMENT 'Number of processed rows',
    `status` Enum8('Processed' = 0, 'Failed' = 1) COMMENT 'Status of the processing file',
    `processing_start_time` Nullable(DateTime) COMMENT 'Time of the start of processing the file',
    `processing_end_time` Nullable(DateTime) COMMENT 'Time of the end of processing the file',
    `exception` String COMMENT 'Exception message if happened'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
COMMENT 'Contains logging entries with the information files processes by S3Queue engine.'

```

Пример:

```sql
SELECT *
FROM system.azure_queue_log
LIMIT 1
FORMAT Vertical

Row 1:
──────
hostname:              clickhouse
event_date:            2024-12-16
event_time:            2024-12-16 13:42:47
database:              default
table:                 azure_queue_engine_table
uuid:                  1bc52858-00c0-420d-8d03-ac3f189f27c8
file_name:             test_1.csv
rows_processed:        3
status:                Processed
processing_start_time: 2024-12-16 13:42:47
processing_end_time:   2024-12-16 13:42:47
exception:

1 row in set. Elapsed: 0.002 sec.

```