---
description: 'This engine provides an integration with the Azure Blob Storage ecosystem,
  allowing streaming data import.'
sidebar_label: 'AzureQueue'
sidebar_position: 181
slug: /engines/table-engines/integrations/azure-queue
title: 'AzureQueue Table Engine'
---

# AzureQueue Table Engine

This engine provides an integration with the [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) ecosystem, allowing streaming data import.

## Create Table {#creating-a-table}

```sql
CREATE TABLE test (name String, value UInt32)
    ENGINE = AzureQueue(...)
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    ...
```

**Engine parameters**

`AzureQueue` parameters are the same as `AzureBlobStorage` table engine supports. See parameters section [here](../../../engines/table-engines/integrations/azureBlobStorage.md).

Similar to the [AzureBlobStorage](/engines/table-engines/integrations/azureBlobStorage) table engine, users can use Azurite emulator for local Azure Storage development. Further details [here](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage).

**Example**

```sql
CREATE TABLE azure_queue_engine_table
(
    `key` UInt64,
    `data` String
)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
SETTINGS mode = 'unordered'
```

## Settings {#settings}

The set of supported settings is the same as for `S3Queue` table engine, but without `s3queue_` prefix. See [full list of settings settings](../../../engines/table-engines/integrations/s3queue.md#settings).
To get a list of settings, configured for the table, use `system.azure_queue_settings` table. Available from `24.10`.

## Description {#description}

`SELECT` is not particularly useful for streaming import (except for debugging), because each file can be imported only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1.  Use the engine to create a table for consuming from specified path in S3 and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background.

Example:

```sql
CREATE TABLE azure_queue_engine_table (key UInt64, data String)
  ENGINE=AzureQueue('<endpoint>', 'CSV', 'gzip')
  SETTINGS
      mode = 'unordered';

CREATE TABLE stats (key UInt64, data String)
  ENGINE = MergeTree() ORDER BY key;

CREATE MATERIALIZED VIEW consumer TO stats
  AS SELECT key, data FROM azure_queue_engine_table;

SELECT * FROM stats ORDER BY key;
```

## Virtual columns {#virtual-columns}

- `_path` — Path to the file.
- `_file` — Name of the file.

For more information about virtual columns see [here](../../../engines/table-engines/index.md#table_engines-virtual_columns).

## Introspection {#introspection}

Enable logging for the table via the table setting `enable_logging_to_s3queue_log=1`.

Introspection capabilities are the same as the [S3Queue table engine](/engines/table-engines/integrations/s3queue#introspection) with several distinct differences:

1. Use the `system.azure_queue` for the in-memory state of the queue for server versions >= 25.1. For older versions use the `system.s3queue` (it would contain information for `azure` tables as well).
2. Enable the `system.azure_queue_log` via the main ClickHouse configuration e.g.

  ```xml
  <azure_queue_log>
    <database>system</database>
    <table>azure_queue_log</table>
  </azure_queue_log>
  ```

This persistent table has the same information as `system.s3queue`, but for processed and failed files.

The table has the following structure:

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
SETTINGS index_granularity = 8192
COMMENT 'Contains logging entries with the information files processes by S3Queue engine.'

```

Example:

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
