---
slug: /en/engines/table-engines/integrations/s3queue
sidebar_position: 181
sidebar_label: S3Queue
---

# S3Queue Table Engine

This engine provides integration with [Amazon S3](https://aws.amazon.com/s3/) ecosystem and allows streaming import. This engine is similar to the [Kafka](../../../engines/table-engines/integrations/kafka.md), [RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md) engines, but provides S3-specific features.

## Create Table {#creating-a-table}

``` sql
CREATE TABLE s3_queue_engine_table (name String, value UInt32)
    ENGINE = S3Queue(path, [NOSIGN, | aws_access_key_id, aws_secret_access_key,] format, [compression])
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    [loading_retries = 0,]
    [processing_threads_num = 1,]
    [enable_logging_to_s3queue_log = 0,]
    [polling_min_timeout_ms = 1000,]
    [polling_max_timeout_ms = 10000,]
    [polling_backoff_ms = 0,]
    [tracked_file_ttl_sec = 0,]
    [tracked_files_limit = 1000,]
    [cleanup_interval_min_ms = 10000,]
    [cleanup_interval_max_ms = 30000,]
```

:::warning
Before `24.7`, it is required to use `s3queue_` prefix for all settings apart from `mode`, `after_processing` and `keeper_path`.
:::

**Engine parameters**

`S3Queue` parameters are the same as `S3` table engine supports. See parameters section [here](../../../engines/table-engines/integrations/s3.md#parameters).

**Example**

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered';
```

Using named collections:

``` xml
<clickhouse>
    <named_collections>
        <s3queue_conf>
            <url>'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*</url>
            <access_key_id>test<access_key_id>
            <secret_access_key>test</secret_access_key>
        </s3queue_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue(s3queue_conf, format = 'CSV', compression_method = 'gzip')
SETTINGS
    mode = 'ordered';
```

## Settings {#settings}

### mode {#mode}

Possible values:

- unordered — With unordered mode, the set of all already processed files is tracked with persistent nodes in ZooKeeper.
- ordered — With ordered mode, the files are processed in lexicographic order. It means that if file named 'BBB' was processed at some point and later on a file named 'AA' is added to the bucket, it will be ignored. Only the max name (in lexicographic sense) of the successfully consumed file, and the names of files that will be retried after unsuccessful loading attempt are being stored in ZooKeeper.

Default value: `ordered` in versions before 24.6. Starting with 24.6 there is no default value, the setting becomes required to be specified manually. For tables created on earlier versions the default value will remain `Ordered` for compatibility.

### after_processing {#after_processing}

Delete or keep file after successful processing.
Possible values:

- keep.
- delete.

Default value: `keep`.

### keeper_path {#keeper_path}

The path in ZooKeeper can be specified as a table engine setting or default path can be formed from the global configuration-provided path and table UUID.
Possible values:

- String.

Default value: `/`.

### s3queue_loading_retries {#loading_retries}

Retry file loading up to specified number of times. By default, there are no retries.
Possible values:

- Positive integer.

Default value: `0`.

### s3queue_processing_threads_num {#processing_threads_num}

Number of threads to perform processing. Applies only for `Unordered` mode.

Default value: `1`.

### s3queue_enable_logging_to_s3queue_log {#enable_logging_to_s3queue_log}

Enable logging to `system.s3queue_log`.

Default value: `0`.

### s3queue_polling_min_timeout_ms {#polling_min_timeout_ms}

Minimal timeout before next polling (in milliseconds).

Possible values:

- Positive integer.

Default value: `1000`.

### s3queue_polling_max_timeout_ms {#polling_max_timeout_ms}

Maximum timeout before next polling (in milliseconds).

Possible values:

- Positive integer.

Default value: `10000`.

### s3queue_polling_backoff_ms {#polling_backoff_ms}

Polling backoff (in milliseconds).

Possible values:

- Positive integer.

Default value: `0`.

### s3queue_tracked_files_limit {#tracked_files_limit}

Allows to limit the number of Zookeeper nodes if the 'unordered' mode is used, does nothing for 'ordered' mode.
If limit reached the oldest processed files will be deleted from ZooKeeper node and processed again.

Possible values:

- Positive integer.

Default value: `1000`.

### s3queue_tracked_file_ttl_sec {#tracked_file_ttl_sec}

Maximum number of seconds to store processed files in ZooKeeper node (store forever by default) for 'unordered' mode, does nothing for 'ordered' mode.
After the specified number of seconds, the file will be re-imported.

Possible values:

- Positive integer.

Default value: `0`.

### s3queue_cleanup_interval_min_ms {#cleanup_interval_min_ms}

For 'Ordered' mode. Defines a minimum boundary for reschedule interval for a background task, which is responsible for maintaining tracked file TTL and maximum tracked files set.

Default value: `10000`.

### s3queue_cleanup_interval_max_ms {#cleanup_interval_max_ms}

For 'Ordered' mode. Defines a maximum boundary for reschedule interval for a background task, which is responsible for maintaining tracked file TTL and maximum tracked files set.

Default value: `30000`.

### s3queue_buckets {#buckets}

For 'Ordered' mode. Available since `24.6`. If there are several replicas of S3Queue table, each working with the same metadata directory in keeper, the value of `s3queue_buckets` needs to be equal to at least the number of replicas. If `s3queue_processing_threads` setting is used as well, it makes sense to increase the value of `s3queue_buckets` setting even further, as it defines the actual parallelism of `S3Queue` processing.

## S3-related Settings {#s3-settings}

Engine supports all s3 related settings. For more information about S3 settings see [here](../../../engines/table-engines/integrations/s3.md).


## S3Queue Ordered mode {#ordered-mode}

`S3Queue` processing mode allows to store less metadata in ZooKeeper, but has a limitation that files, which added later by time, are required to have alphanumerically bigger names.

`S3Queue` `ordered` mode, as well as `unordered`, supports `(s3queue_)processing_threads_num` setting (`s3queue_` prefix is optional), which allows to control number of threads, which would do processing of `S3` files locally on the server.
In addition, `ordered` mode also introduces another setting called `(s3queue_)buckets` which means "logical threads". It means that in distributed scenario, when there are several servers with `S3Queue` table replicas, where this setting defines the number of processing units. E.g. each processing thread on each `S3Queue` replica will try to lock a certain `bucket` for processing, each `bucket` is attributed to certain files by hash of the file name. Therefore, in distributed scenario it is highly recommended to have `(s3queue_)buckets` setting to be at least equal to the number of replicas or bigger. This is fine to have the number of buckets bigger than the number of replicas. The most optimal scenario would be for `(s3queue_)buckets` setting to equal a multiplication of `number_of_replicas` and `(s3queue_)processing_threads_num`.
The setting `(s3queue_)processing_threads_num` is not recommended for usage before version `24.6`.
The setting `(s3queue_)buckets` is available starting with version `24.6`.

## Description {#description}

`SELECT` is not particularly useful for streaming import (except for debugging), because each file can be imported only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1.  Use the engine to create a table for consuming from specified path in S3 and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background.

Example:

``` sql
  CREATE TABLE s3queue_engine_table (name String, value UInt32)
    ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
    SETTINGS
        mode = 'unordered';

  CREATE TABLE stats (name String, value UInt32)
    ENGINE = MergeTree() ORDER BY name;

  CREATE MATERIALIZED VIEW consumer TO stats
    AS SELECT name, value FROM s3queue_engine_table;

  SELECT * FROM stats ORDER BY name;
```

## Virtual columns {#virtual-columns}

- `_path` — Path to the file.
- `_file` — Name of the file.

For more information about virtual columns see [here](../../../engines/table-engines/index.md#table_engines-virtual_columns).


## Wildcards In Path {#wildcards-in-path}

`path` argument can specify multiple files using bash-like wildcards. For being processed file should exist and match to the whole path pattern. Listing of files is determined during `SELECT` (not at `CREATE` moment).

- `*` — Substitutes any number of any characters except `/` including empty string.
- `**` — Substitutes any number of any characters include `/` including empty string.
- `?` — Substitutes any single character.
- `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
- `{N..M}` — Substitutes any number in range from N to M including both borders. N and M can have leading zeroes e.g. `000..078`.

Constructions with `{}` are similar to the [remote](../../../sql-reference/table-functions/remote.md) table function.

## Limitations {#limitations}

1. Duplicated rows can be as a result of:

- an exception happens during parsing in the middle of file processing and retries are enabled via `s3queue_loading_retries`;

- `S3Queue` is configured on multiple servers pointing to the same path in zookeeper and keeper session expires before one server managed to commit processed file, which could lead to another server taking processing of the file, which could be partially or fully processed by the first server;

- abnormal server termination.

2. `S3Queue` is configured on multiple servers pointing to the same path in zookeeper and `Ordered` mode is used, then `s3queue_loading_retries` will not work. This will be fixed soon.


## Introspection {#introspection}

For introspection use `system.s3queue` stateless table and `system.s3queue_log` persistent table.

1. `system.s3queue`. This table is not persistent and shows in-memory state of `S3Queue`: which files are currently being processed, which files are processed or failed.

``` sql
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue
(
    `database` String,
    `table` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` String,
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64)
    `exception` String
)
ENGINE = SystemS3Queue
COMMENT 'Contains in-memory state of S3Queue metadata and currently processed rows per file.' │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Example:

``` sql

SELECT *
FROM system.s3queue

Row 1:
──────
zookeeper_path:        /clickhouse/s3queue/25ea5621-ae8c-40c7-96d0-cec959c5ab88/3b3f66a1-9866-4c2e-ba78-b6bfa154207e
file_name:             wikistat/original/pageviews-20150501-030000.gz
rows_processed:        5068534
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:31
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5068534,'SelectedBytes':198132283,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':2480,'S3QueueSetFileProcessedMicroseconds':9985,'S3QueuePullMicroseconds':273776,'LogTest':17}
exception:
```

2. `system.s3queue_log`. Persistent table. Has the same information as `system.s3queue`, but for `processed` and `failed` files.

The table has the following structure:

``` sql
SHOW CREATE TABLE system.s3queue_log

Query id: 0ad619c3-0f2a-4ee4-8b40-c73d86e04314

┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_log
(
    `event_date` Date,
    `event_time` DateTime,
    `table_uuid` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` Enum8('Processed' = 0, 'Failed' = 1),
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64),
    `exception` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
SETTINGS index_granularity = 8192 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

In order to use `system.s3queue_log` define its configuration in server config file:

``` xml
    <s3queue_log>
        <database>system</database>
        <table>s3queue_log</table>
    </s3queue_log>
```

Example:

``` sql
SELECT *
FROM system.s3queue_log

Row 1:
──────
event_date:            2023-10-13
event_time:            2023-10-13 13:10:12
table_uuid:
file_name:             wikistat/original/pageviews-20150501-020000.gz
rows_processed:        5112621
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:12
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5112621,'SelectedBytes':198577687,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':1934,'S3QueueSetFileProcessedMicroseconds':17063,'S3QueuePullMicroseconds':5841972,'LogTest':17}
exception:
```
