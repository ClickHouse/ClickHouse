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
Creating `Paimon*` tables is gated by `allow_experimental_paimon_storage_engine` (disabled by default), so enable it before running `CREATE TABLE`.

```sql
SET allow_experimental_paimon_storage_engine = 1;

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

- `allow_experimental_paimon_storage_engine` — enables creation of `Paimon`, `PaimonS3`, `PaimonAzure`, `PaimonHDFS`, and `PaimonLocal` table engines. Default: `0` (disabled).
- `paimon_incremental_read` — enable incremental read mode.
- `paimon_metadata_refresh_interval_sec` — background metadata refresh interval in seconds. When set to a value greater than 0, a background task periodically pulls the latest snapshot and schema from object storage. Default: 30.
- `paimon_keeper_path` — Keeper path for incremental read state. Must be set and unique per table; supports macros such as `{database}`, `{table}`, `{uuid}`.
- `paimon_replica_name` — Replica name for incremental read state. Must be set and unique per replica; supports macros such as `{replica}`.

## Incremental read examples {#incremental-read-examples}

Incremental read with Keeper state:

```sql
CREATE TABLE paimon_inc
ENGINE = PaimonS3(paimon_conf, filename = 'paimon_all_types')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/{database}/{uuid}',
    paimon_replica_name = '{replica}';
```

### Query-level settings for incremental read {#query-level-settings-for-incremental-read}

The following settings are **query-level** (passed via `SELECT ... SETTINGS`, not in `CREATE TABLE`). They control per-query behavior of incremental reads:

- `paimon_target_snapshot_id` — read only the delta of the specified snapshot. The committed watermark in Keeper is **not** advanced, so the same snapshot can be re-read any number of times. Default: `-1` (disabled).
- `max_consume_snapshots` — maximum number of snapshots to consume in a single incremental read. When the source has accumulated many unread snapshots, this limits how many are consumed per query to control batch size. `0` means no limit. Default: `0`.

**Targeted snapshot read** — always returns the delta of snapshot 1, regardless of the current watermark:

```sql
SELECT count()
FROM paimon_inc
SETTINGS paimon_target_snapshot_id = 1;
```

**Limiting snapshots per batch** — if three new snapshots are pending, consume at most two per query:

```sql
SELECT count()
FROM paimon_inc
SETTINGS max_consume_snapshots = 2;
```

## Paimon to MergeTree via Refreshable Materialized View {#paimon-to-mergetree-via-refresh-mv}

You can build an end-to-end pipeline that continuously syncs data from a Paimon table into a MergeTree table using a refreshable Materialized View in `APPEND` mode. Each refresh cycle reads only new incremental data from Paimon and appends it to the destination table.

**Step 1 — Create the Paimon source table with incremental read and metadata refresh enabled.**

The example below uses `PaimonLocal`. Replace the engine with `PaimonS3`, `PaimonAzure`, `PaimonHDFS`, or the `Paimon` alias as appropriate for your storage backend:

```sql
SET allow_experimental_paimon_storage_engine = 1;

-- Local storage
CREATE TABLE paimon_mv_source
ENGINE = PaimonLocal('/path/to/paimon/table')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;

-- S3 storage (Paimon is an alias for PaimonS3)
CREATE TABLE paimon_mv_source
ENGINE = Paimon('http://minio:9000/bucket/path/to/table', 'access_key', 'secret_key')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;
```

`paimon_metadata_refresh_interval_sec` sets the background metadata refresh interval in seconds. When greater than 0, a background task periodically pulls the latest snapshot and schema from object storage, so that the MV refresh cycle can see newly committed data without waiting for a query to trigger the metadata update. Default is 30. Use cautiously on many tables to avoid excessive object storage and Keeper I/O.

**Step 2 — Create the MergeTree destination table (schema cloned from the Paimon table):**

```sql
CREATE TABLE paimon_mv_dest AS paimon_mv_source
ENGINE = MergeTree()
ORDER BY tuple();
```

**Step 3 — Create the refreshable Materialized View:**

```sql
CREATE MATERIALIZED VIEW paimon_mv
REFRESH EVERY 10 SECOND
APPEND
TO paimon_mv_dest
AS SELECT * FROM paimon_mv_source;
```

Every 10 seconds the MV fires a `SELECT * FROM paimon_mv_source`, which returns only the rows added since the last committed snapshot, and appends them to `paimon_mv_dest`.

**Cleanup:**

```sql
SYSTEM STOP VIEW paimon_mv;
DROP VIEW IF EXISTS paimon_mv SYNC;
DROP TABLE IF EXISTS paimon_mv_dest SYNC;
DROP TABLE IF EXISTS paimon_mv_source SYNC;
```

:::note
Stop the MV before dropping it to prevent background refresh from blocking DDL operations.
:::

## Limitations {#limitations}

- Incremental read requires Keeper (ZooKeeper) to be configured.
- Incremental read requires `paimon_keeper_path` to be set and unique per table.
- `paimon_replica_name` must be unique per replica within the same Keeper path.
- Incremental read uses at-most-once delivery: the committed snapshot is advanced when data files are collected, before the data is actually consumed. If the query fails after file collection, the skipped snapshots will not be re-read on retry.
- The table engine is read-only; data modification is not supported.
- Incremental read does not handle historical data deletions from the Paimon source. If upstream Paimon data is deleted or updated, the corresponding rows already written to a ClickHouse MergeTree destination table will not be automatically removed. You must manually issue `ALTER TABLE ... DELETE` on the MergeTree table to clean up stale data.

## Aliases {#aliases}

Table engine `Paimon` is an alias to `PaimonS3` now.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the file. Type: `LowCardinality(String)`.
- `_file` — Name of the file. Type: `LowCardinality(String)`.
- `_size` — Size of the file in bytes. Type: `Nullable(UInt64)`. If the file size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_etag` — The etag of the file. Type: `LowCardinality(String)`. If the etag is unknown, the value is `NULL`.

## Data Types supported {#data-types-supported}

| Paimon Data Type | ClickHouse Data Type |
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
