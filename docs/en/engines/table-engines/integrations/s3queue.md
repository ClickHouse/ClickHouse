---
slug: /en/engines/table-engines/integrations/s3queue
sidebar_position: 7
sidebar_label: S3Queue
---

# S3Queue Table Engine
This engine provides integration with [Amazon S3](https://aws.amazon.com/s3/) ecosystem and allows streaming import. This engine is similar to the [Kafka](../../../engines/table-engines/integrations/kafka.md), [RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md) engines, but provides S3-specific features.

## Create Table {#creating-a-table}

``` sql
CREATE TABLE s3_queue_engine_table (name String, value UInt32)
    ENGINE = S3Queue(path [, NOSIGN | aws_access_key_id, aws_secret_access_key,] format, [compression])
    [SETTINGS]
    [mode = 'unordered',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    [s3queue_loading_retries = 0,]
    [s3queue_polling_min_timeout_ms = 1000,]
    [s3queue_polling_max_timeout_ms = 10000,]
    [s3queue_polling_backoff_ms = 0,]
    [s3queue_tracked_files_limit = 1000,]
    [s3queue_tracked_file_ttl_sec = 0,]
    [s3queue_polling_size = 50,]
```

**Engine parameters**

- `path` — Bucket url with path to file. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings. For more information see [below](#wildcards-in-path).
- `NOSIGN` - If this keyword is provided in place of credentials, all the requests will not be signed.
- `format` — The [format](../../../interfaces/formats.md#formats) of the file.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. Parameter is optional. If credentials are not specified, they are used from the configuration file. For more information see [Using S3 for Data Storage](../mergetree-family/mergetree.md#table_engine-mergetree-s3).
- `compression` — Compression type. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Parameter is optional. By default, it will autodetect compression by file extension.

**Example**

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'ordered';
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

## Settings {#s3queue-settings}

### mode {#mode}

Possible values:

- unordered — With unordered mode, the set of all already processed files is tracked with persistent nodes in ZooKeeper.
- ordered — With ordered mode, only the max name of the successfully consumed file, and the names of files that will be retried after unsuccessful loading attempt are being stored in ZooKeeper.

Default value: `unordered`.

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

### s3queue_loading_retries {#s3queue_loading_retries}

Retry file loading up to specified number of times. By default, there are no retries.
Possible values:

- Positive integer.

Default value: `0`.

### s3queue_polling_min_timeout_ms {#s3queue_polling_min_timeout_ms}

Minimal timeout before next polling (in milliseconds).

Possible values:

- Positive integer.

Default value: `1000`.

### s3queue_polling_max_timeout_ms {#s3queue_polling_max_timeout_ms}

Maximum timeout before next polling (in milliseconds).

Possible values:

- Positive integer.

Default value: `10000`.

### s3queue_polling_backoff_ms {#s3queue_polling_backoff_ms}

Polling backoff (in milliseconds).

Possible values:

- Positive integer.

Default value: `0`.

### s3queue_tracked_files_limit {#s3queue_tracked_files_limit}

Allows to limit the number of Zookeeper nodes if the 'unordered' mode is used, does nothing for 'ordered' mode.
If limit reached the oldest processed files will be deleted from ZooKeeper node and processed again.

Possible values:

- Positive integer.

Default value: `1000`.

### s3queue_tracked_file_ttl_sec {#s3queue_tracked_file_ttl_sec}

Maximum number of seconds to store processed files in ZooKeeper node (store forever by default) for 'unordered' mode, does nothing for 'ordered' mode.
After the specified number of seconds, the file will be re-imported.

Possible values:

- Positive integer.

Default value: `0`.

### s3queue_polling_size {#s3queue_polling_size}

Maximum files to fetch from S3 with SELECT or in background task.
Engine takes files for processing from S3 in batches.
We limit the batch size to increase concurrency if multiple table engines with the same `keeper_path` consume files from the same path.

Possible values:

- Positive integer.

Default value: `50`.


## S3-related Settings {#s3-settings}

Engine supports all s3 related settings. For more information about S3 settings see [here](../../../engines/table-engines/integrations/s3.md).


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
        mode = 'unordered',
        keeper_path = '/clickhouse/s3queue/';

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

:::note
If the listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::
