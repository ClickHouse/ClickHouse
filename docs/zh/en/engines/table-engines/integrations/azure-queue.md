---
description: '该引擎可与 Azure Blob 存储生态系统集成，
  支持流式数据导入。'
sidebar_label: 'AzureQueue'
sidebar_position: 181
slug: /engines/table-engines/integrations/azure-queue
title: 'AzureQueue 表引擎'
doc_type: 'reference'
---

该引擎可与 [Azure Blob 存储](https://azure.microsoft.com/en-us/products/storage/blobs) 生态系统集成，支持流式数据导入。

<div id="creating-a-table">
  ## CREATE 表
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

**引擎参数**

`AzureQueue` 的参数与 `AzureBlobStorage` 表引擎支持的参数相同。请参阅[此处](../../../engines/table-engines/integrations/azureBlobStorage.md)的参数部分。

与 [AzureBlobStorage](/zh/engines/table-engines/integrations/azureBlobStorage) 表引擎类似，用户也可以使用 Azurite 模拟器在本地进行 Azure Storage 开发。更多详情请参阅[此处](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage)。

**示例**

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
  ## 设置
</div>

支持的设置项与 `S3Queue` 表引擎基本相同，但不带 `s3queue_` 前缀。请参见[完整设置项列表](../../../engines/table-engines/integrations/s3queue.md#settings)。
如需查看为该表配置的设置项列表，请使用 `system.azure_queue_settings` 表。自 `24.10` 起可用。

以下设置项仅兼容 AzureQueue，不适用于 S3Queue。

<div id="after_processing_move_connection_string">
  ### `after_processing_move_connection_string`
</div>

如果目标端是另一个 Azure 容器，则用于将成功处理后的文件移动到该目标端的 Azure Blob 存储连接字符串。

可能的值：

* String。

默认值：空字符串。

<div id="after_processing_move_container">
  ### `after_processing_move_container`
</div>

如果目标端是另一个 Azure 容器，用于存放成功处理后文件的容器名称。

可能的值：

* String。

默认值：空字符串。

示例：

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
  ## 在 AzureQueue 表引擎 上执行 SELECT
</div>

默认情况下，AzureQueue 表禁止执行 SELECT 查询。这遵循常见的队列模式：数据读取一次后就会从队列中移除。禁止 SELECT 是为了防止意外数据丢失。
不过，在某些情况下，这样做可能会很有用。为此，你需要将设置 `stream_like_engine_allow_direct_select` 设为 `True`。
AzureQueue engine 针对 SELECT 查询提供了一个特殊设置：`commit_on_select`。将其设为 `False` 可在读取后保留队列中的数据，设为 `True` 则会将其移除。

<div id="description">
  ## 描述
</div>

`SELECT` 对流式导入并不是特别有用 (调试除外) ，因为每个文件只能导入一次。更实用的做法是使用 [materialized views](../../../sql-reference/statements/create/view.md) 创建实时处理流程。为此，请执行以下操作：

1. 使用该引擎创建一个表，用于消费 Azure Blob 存储中指定路径的数据，并将其视为数据 stream。
2. 创建一个具有所需结构的表。
3. 创建一个 materialized view，将来自该引擎的数据转换后写入先前创建的表。

当 `MATERIALIZED VIEW` 与该引擎关联后，它会开始在后台收集数据。

引擎参数的形式为 `AzureQueue(connection_string, container_name, blobpath, format[, compression])`。

示例：

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
  ## 虚拟列
</div>

* `_path` — 文件路径。
* `_file` — 文件名。

有关虚拟列的更多信息，见[此处](../../../engines/table-engines/index.md#table_engines-virtual_columns)。

<div id="introspection">
  ## 内部信息
</div>

通过表设置 `enable_logging_to_queue_log=1` 为该表启用日志记录。

内部信息功能与 [S3Queue 表引擎](/zh/engines/table-engines/integrations/s3queue#introspection) 相同，但有以下几个明显区别：

1. 对于 server 版本 &gt;= 25.1，队列的内存状态使用 `system.azure_queue_metadata_cache`。对于较早版本，请使用 `system.s3queue_metadata_cache` (其中也会包含 `azure` 表的信息) 。
2. 通过 ClickHouse 主配置启用 `system.azure_queue_log`，例如：

```xml
  <azure_queue_log>
    <database>system</database>
    <table>azure_queue_log</table>
  </azure_queue_log>
```

这个持久化表包含与 `system.s3queue_metadata_cache` 相同的信息，但用于已处理和失败的文件。

该表的结构如下：

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

示例：

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