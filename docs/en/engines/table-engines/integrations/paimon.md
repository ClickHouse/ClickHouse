---
description: 'This engine provides a read-only integration with existing Apache Paimon
  tables in Amazon S3, Azure, HDFS and locally stored tables.'
sidebar_label: 'Paimon'
sidebar_position: 95
slug: /engines/table-engines/integrations/paimon
title: 'Paimon table engine'
doc_type: 'reference'
---

# Paimon table engine {#paimon-table-engine}

This engine provides a read-only integration with existing Apache [Paimon](https://paimon.apache.org/) tables in Amazon S3, Azure, HDFS and locally stored tables.
It supports snapshot reads, incremental reads, and basic partition pruning provided by the engine.

## Create table {#create-table}

Note that the Paimon table must already exist in the storage, this command does not take DDL parameters to create a new table.

```sql
CREATE TABLE paimon_table_s3
    ENGINE = PaimonS3(url,  [, access_key_id, secret_access_key] [,format] [,structure] [,compression])

CREATE TABLE paimon_table_azure
    ENGINE = PaimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

CREATE TABLE paimon_table_hdfs
    ENGINE = PaimonHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE paimon_table_local
    ENGINE = PaimonLocal(path_to_table, [,format] [,compression_method])
```

## Engine arguments {#engine-arguments}

Description of the arguments coincides with description of arguments in engines `S3`, `AzureBlobStorage`, `HDFS` and `File` correspondingly.
`format` stands for the format of data files in the Paimon table.

Engine parameters can be specified using [Named Collections](../../../operations/named-collections.md)

### Example {#example}

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Using named collections:

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3(paimon_conf, filename = 'test_table')
```

## Capabilities {#capabilities}

- Snapshot reads from the latest table snapshot.
- Incremental reads based on committed snapshot id when enabled.
- Partition pruning when `use_paimon_partition_pruning` is enabled.
- Optional background refresh of metadata when configured.
- Stable table UUID when using Atomic/Replicated databases, enabling `{uuid}` macros in Keeper paths.

## Settings {#settings}

This engine uses the same settings as the corresponding object storage engines and adds Paimon-specific settings:

- `paimon_incremental_read` — enable incremental read mode.
- `paimon_metadata_refresh_interval_ms` — refresh metadata in background.
- `paimon_target_snapshot_id` — read a specific snapshot delta (session-level).
- `paimon_keeper_path` — Keeper path for incremental read state. Must be set and unique per table; supports macros such as `{database}`, `{table}`, `{uuid}`.
- `paimon_replica_name` — Replica name for incremental read state. Must be set and unique per replica; supports macros such as `{replica}`.
- `use_paimon_partition_pruning` — enable partition pruning for Paimon.

## Limitations {#limitations}

- Incremental read requires Keeper (ZooKeeper) to be configured.
- Incremental read requires `paimon_keeper_path` to be set and unique per table.
- `paimon_replica_name` must be unique per replica within the same Keeper path.
- The table engine is read-only; data modification is not supported.

## Aliases {#aliases}

Table engine `Paimon` is an alias to `PaimonS3` now.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Data Types supported {#data-types-supported}

| Paimon Data Type | ClickHouse Data Type 
|-------|--------|
|BOOLEAN     |Int8      |
|TINYINT     |Int8      |
|SMALLINT     |Int16      |
|INTEGER     |Int32      |
|BIGINT     |Int64      |
|FLOAT     |Float32      |
|DOUBLE     |Float64      |
|STRING,VARCHAR,BYTES,VARBINARY     |String      |
|DATE     |Date      |
|TIME(p),TIME     |Time('UTC')      |
|TIMESTAMP(p) WITH LOCAL TIME ZONE     |DateTime64      |
|TIMESTAMP(p)     |DateTime64('UTC')      |
|CHAR     |FixedString(1)      |
|BINARY(n)     |FixedString(n)      |
|DECIMAL(P,S)     |Decimal(P,S)      |
|ARRAY     |Array      |
|MAP     |Map    |

## Partition supported {#partition-supported}

Data types supported in Paimon partition keys:
* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`
