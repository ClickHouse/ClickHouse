---
slug: /en/engines/table-engines/special/filelog
sidebar_position: 160
sidebar_label: FileLog
---

# FileLog Engine {#filelog-engine}

This engine allows to process application log files as a stream of records.

`FileLog` lets you:

- Subscribe to log files.
- Process new records as they are appended to subscribed log files.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = FileLog('path_to_logs', 'format_name') SETTINGS
    [poll_timeout_ms = 0,]
    [poll_max_batch_size = 0,]
    [max_block_size = 0,]
    [max_threads = 0,]
    [poll_directory_watch_events_backoff_init = 500,]
    [poll_directory_watch_events_backoff_max = 32000,]
    [poll_directory_watch_events_backoff_factor = 2,]
    [handle_error_mode = 'default']
```

Engine arguments:

- `path_to_logs` – Path to log files to subscribe. It can be path to a directory with log files or to a single log file. Note that ClickHouse allows only paths inside `user_files` directory.
- `format_name` - Record format. Note that FileLog process each line in a file as a separate record and not all data formats are suitable for it.

Optional parameters:

- `poll_timeout_ms` - Timeout for single poll from log file. Default: [stream_poll_timeout_ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
- `poll_max_batch_size` — Maximum amount of records to be polled in a single poll. Default: [max_block_size](../../../operations/settings/settings.md#setting-max_block_size).
- `max_block_size` — The maximum batch size (in records) for poll. Default: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size).
- `max_threads` - Number of max threads to parse files, default is 0, which means the number will be max(1, physical_cpu_cores / 4).
- `poll_directory_watch_events_backoff_init` - The initial sleep value for watch directory thread. Default: `500`.
- `poll_directory_watch_events_backoff_max` - The max sleep value for watch directory thread. Default: `32000`.
- `poll_directory_watch_events_backoff_factor` - The speed of backoff, exponential by default. Default: `2`.
- `handle_error_mode` — How to handle errors for FileLog engine. Possible values: default (the exception will be thrown if we fail to parse a message), stream (the exception message and raw message will be saved in virtual columns `_error` and `_raw_message`).

## Description {#description}

The delivered records are tracked automatically, so each record in a log file is only counted once.

`SELECT` is not particularly useful for reading records (except for debugging), because each record can be read only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1.  Use the engine to create a FileLog table and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive records from log files and convert them to the required format using `SELECT`.
One FileLog table can have as many materialized views as you like, they do not read data from the table directly, but receive new records (in blocks), this way you can write to several tables with different detail level (with grouping - aggregation and without).

Example:

``` sql
  CREATE TABLE logs (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = FileLog('user_files/my_app/app.log', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() as total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

To stop receiving streams data or to change the conversion logic, detach the materialized view:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

If you want to change the target table by using `ALTER`, we recommend disabling the material view to avoid discrepancies between the target table and the data from the view.

## Virtual Columns {#virtual-columns}

- `_filename` - Name of the log file. Data type: `LowCardinality(String)`.
- `_offset` - Offset in the log file. Data type: `UInt64`.

Additional virtual columns when `handle_error_mode='stream'`:

- `_raw_record` - Raw record that couldn't be parsed successfully. Data type: `Nullable(String)`.
- `_error` - Exception message happened during failed parsing. Data type: `Nullable(String)`.

Note: `_raw_record` and `_error` virtual columns are filled only in case of exception during parsing, they are always `NULL` when message was parsed successfully.
