---
slug: /en/engines/table-engines/integrations/azure-queue
sidebar_position: 181
sidebar_label: AzureQueue
---

# AzureQueue Table Engine

This engine provides an integration with [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) ecosystem, allowing streaming data import.

## Create Table {#creating-a-table}

``` sql
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

**Example**

```sql
CREATE TABLE azure_queue_engine_table (name String, value UInt32)
ENGINE=AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/data/')
SETTINGS
    mode = 'unordered'
```

## Settings {#settings}

The set of supported settings is the same as for `S3Queue` table engine, but without `s3queue_` prefix. See [full list of settings settings](../../../engines/table-engines/integrations/s3queue.md#settings).

## Description {#description}

`SELECT` is not particularly useful for streaming import (except for debugging), because each file can be imported only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1.  Use the engine to create a table for consuming from specified path in S3 and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background.

Example:

``` sql
  CREATE TABLE azure_queue_engine_table (name String, value UInt32)
    ENGINE=AzureQueue('<endpoint>', 'CSV', 'gzip')
    SETTINGS
        mode = 'unordered';

  CREATE TABLE stats (name String, value UInt32)
    ENGINE = MergeTree() ORDER BY name;

  CREATE MATERIALIZED VIEW consumer TO stats
    AS SELECT name, value FROM azure_queue_engine_table;

  SELECT * FROM stats ORDER BY name;
```

## Virtual columns {#virtual-columns}

- `_path` — Path to the file.
- `_file` — Name of the file.

For more information about virtual columns see [here](../../../engines/table-engines/index.md#table_engines-virtual_columns).
