---
description: 'This engine allows integrating ClickHouse with Amazon Kinesis Data Streams.'
sidebar_label: 'Kinesis'
sidebar_position: 85
slug: /engines/table-engines/integrations/kinesis
title: 'Kinesis table engine'
doc_type: 'guide'
---

# Kinesis table engine

:::note
The Kinesis table engine is experimental. Enable it with:
```sql
SET allow_experimental_kinesis_table = 1;
```
:::

This engine allows integrating ClickHouse with [Amazon Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/).

`Kinesis` lets you:

- Consume records from a Kinesis data stream.
- Produce records to a Kinesis data stream.
- Process streams as they become available.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Kinesis SETTINGS
    kinesis_stream_name = 'stream_name',
    kinesis_format = 'data_format'[,]
    [kinesis_aws_access_key_id = '',]
    [kinesis_aws_secret_access_key = '',]
    [kinesis_aws_region = 'us-east-1',]
    [kinesis_endpoint = '',]
    [kinesis_verify_ssl = 1,]
    [kinesis_schema = '',]
    [kinesis_num_consumers = N,]
    [kinesis_max_block_size = N,]
    [kinesis_flush_interval_ms = N,]
    [kinesis_poll_timeout_ms = N,]
    [kinesis_max_records_per_request = N,]
    [kinesis_starting_position = 'LATEST',]
    [kinesis_at_timestamp = N,]
    [kinesis_save_checkpoints = 1,]
    [kinesis_skip_broken_messages = N,]
    [kinesis_max_rows_per_message = 1,]
    [kinesis_handle_error_mode = 'default']
```

Required parameters:

- `kinesis_stream_name` – Name of the Kinesis data stream.
- `kinesis_format` – Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](../../../interfaces/formats.md) section.

Optional parameters:

- `kinesis_aws_access_key_id` – AWS access key ID. If empty (together with `kinesis_aws_secret_access_key`), credentials are loaded from the default AWS credential provider chain (environment variables, EC2 instance profile, etc.).
- `kinesis_aws_secret_access_key` – AWS secret access key.
- `kinesis_aws_region` – AWS region where the stream is located. Default: `us-east-1`.
- `kinesis_endpoint` – Custom endpoint URL (for example, a LocalStack instance). If empty, the standard AWS endpoint is used.
- `kinesis_verify_ssl` – Whether to verify the SSL certificate when connecting. Default: `1`.
- `kinesis_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) requires the path to the schema file and the name of the root `schema.capnp:Message` object.
- `kinesis_num_consumers` – The number of consumer threads per table. Each consumer is assigned a subset of the stream's shards. Specify more consumers if the throughput of one consumer is insufficient. Default: `1`.
- `kinesis_max_block_size` – Number of rows collected before flushing data from Kinesis. Default: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size).
- `kinesis_flush_interval_ms` – Timeout for flushing data from Kinesis. Default: [stream_flush_interval_ms](/operations/settings/settings#stream_flush_interval_ms).
- `kinesis_poll_timeout_ms` – Timeout between polling attempts when no records are available. Default: `500`.
- `kinesis_max_records_per_request` – Maximum number of records per `GetRecords` request (between 1 and 10000). Default: `10000`.
- `kinesis_starting_position` – Where to begin reading when no checkpoint is saved. Possible values: `LATEST` (only new records), `TRIM_HORIZON` (from the oldest available record), `AT_TIMESTAMP` (from a specific timestamp). Default: `LATEST`.
- `kinesis_at_timestamp` – Unix timestamp (seconds) used when `kinesis_starting_position = AT_TIMESTAMP`. Default: `0`.
- `kinesis_save_checkpoints` – If enabled, the engine persists per-shard sequence number checkpoints in `system.kinesis_checkpoints`, allowing reads to resume after a restart. Default: `1`.
- `kinesis_skip_broken_messages` – Number of broken (unparseable) records to skip per block before throwing an exception. Default: `0`.
- `kinesis_max_rows_per_message` – The maximum number of rows written in one `PutRecord` call for row-based formats. Default: `1`.
- `kinesis_handle_error_mode` – How to handle errors for the Kinesis engine. Possible values: `default` (an exception is thrown if a record fails to parse), `dead_letter_queue` (error-related data is saved in `system.dead_letter_queue`).

## Description {#description}

`SELECT` is not particularly useful for reading records (except for debugging), because each record can be read only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1. Use the engine to create a Kinesis consumer and consider it a data stream.
2. Create a table with the desired structure.
3. Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive records from Kinesis and convert them to the required format using `SELECT`.

One Kinesis table can have as many materialized views as you like.

Example:

```sql
  CREATE TABLE kinesis_stream
  (
      id UInt64,
      name String,
      ts DateTime
  ) ENGINE = Kinesis SETTINGS
      kinesis_stream_name = 'my-stream',
      kinesis_aws_region = 'us-east-1',
      kinesis_format = 'JSONEachRow',
      kinesis_num_consumers = 4;

  CREATE TABLE events (id UInt64, name String, ts DateTime)
      ENGINE = MergeTree() ORDER BY id;

  CREATE MATERIALIZED VIEW consumer TO events
      AS SELECT id, name, ts FROM kinesis_stream;

  SELECT id, name, ts FROM events ORDER BY id;
```

### Shard distribution {#shard-distribution}

Shards are distributed among consumers using round-robin assignment at startup. Each consumer reads from its assigned shards independently. If the number of shards changes (due to resharding), the table must be restarted to redistribute shards.

### Checkpointing {#checkpointing}

When `kinesis_save_checkpoints = 1`, the engine stores the last successfully consumed sequence number for each shard in the `system.kinesis_checkpoints` table (created automatically). On restart, reading resumes from the saved sequence numbers rather than from `kinesis_starting_position`. To reset consumption to the beginning, truncate or drop `system.kinesis_checkpoints`.

To improve performance, received records are grouped into blocks of size [max_insert_block_size](/operations/settings/settings#max_insert_block_size). If the block was not formed within [stream_flush_interval_ms](../../../operations/server-configuration-parameters/settings.md) milliseconds, the data will be flushed to the table regardless of the completeness of the block.

Do not use the same table for inserts and materialized views.

## Virtual columns {#virtual-columns}

- `_sequence_number` – Kinesis sequence number of the record. Data type: `String`.
- `_partition_key` – Partition key of the record. Data type: `String`.
- `_shard_id` – ID of the shard from which the record was read. Data type: `String`.
- `_approximate_arrival_timestamp` – Unix timestamp (seconds) when the record arrived in the stream. Data type: `UInt64`.

## Caveats {#caveats}

Even though you may specify [default column expressions](/sql-reference/statements/create/table.md/#default_values) (such as `DEFAULT`, `MATERIALIZED`, `ALIAS`) in the table definition, these will be ignored. Instead, the columns will be filled with their respective default type values.

## Data formats support {#data-formats-support}

The Kinesis engine supports all [formats](../../../interfaces/formats.md) supported in ClickHouse.
The number of rows in one Kinesis record depends on whether the format is row-based or block-based:

- For row-based formats the number of rows in one `PutRecord` call can be controlled by setting `kinesis_max_rows_per_message`.
- For block-based formats we cannot divide a block into smaller parts, but the number of rows in one block can be controlled by the general setting [max_block_size](/operations/settings/settings#max_block_size).
