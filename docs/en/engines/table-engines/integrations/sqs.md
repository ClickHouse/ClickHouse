---
description: 'This engine allows integrating ClickHouse with Amazon SQS.'
sidebar_label: 'SQS'
sidebar_position: 175
slug: /engines/table-engines/integrations/sqs
title: 'SQS table engine'
doc_type: 'guide'
---

# SQS table engine

This engine allows integrating ClickHouse with [Amazon Simple Queue Service (SQS)](https://aws.amazon.com/sqs/) and compatible services such as [LocalStack](https://localstack.cloud/).

`SQS` lets you:

- Consume messages from an SQS queue and insert them into ClickHouse tables.
- Publish rows from ClickHouse into an SQS queue.
- Process incoming message streams using materialized views.

## Note
The SQS table engine is experimental. Enable it with:
```sql
SET allow_experimental_sqs_table = 1;
```

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = SQS SETTINGS
    sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
    sqs_format = 'data_format'[,]
    [sqs_aws_access_key_id = '',]
    [sqs_aws_secret_access_key = '',]
    [sqs_aws_region = 'us-east-1',]
    [sqs_endpoint = '',]
    [sqs_schema = '',]
    [sqs_num_consumers = N,]
    [sqs_max_block_size = N,]
    [sqs_flush_interval_ms = N,]
    [sqs_poll_timeout_ms = N,]
    [sqs_max_messages_per_receive = N,]
    [sqs_visibility_timeout = N,]
    [sqs_wait_time_seconds = N,]
    [sqs_auto_delete = true,]
    [sqs_dead_letter_queue_url = '',]
    [sqs_max_receive_count = N,]
    [sqs_skip_broken_messages = N,]
    [sqs_max_rows_per_message = N,]
    [sqs_verify_ssl = true,]
    [sqs_handle_error_mode = 'default']
```

Required parameters:

- `sqs_queue_url` — Full URL of the SQS queue, e.g. `https://sqs.us-east-1.amazonaws.com/123456789012/my-queue`. For LocalStack: `http://localhost:4566/000000000000/my-queue`.
- `sqs_format` — Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](../../../interfaces/formats.md) section.

Optional parameters:

- `sqs_aws_access_key_id` — AWS access key ID. If not set (together with `sqs_aws_secret_access_key`), the engine uses the default AWS credential provider chain (IAM instance profile, ECS/EKS task role, environment variables, shared credentials file). Default: `''`.
- `sqs_aws_secret_access_key` — AWS secret access key. See `sqs_aws_access_key_id`. Default: `''`.
- `sqs_aws_region` — AWS region. Default: `us-east-1`.
- `sqs_endpoint` — Custom endpoint URL override. Useful for LocalStack or other SQS-compatible services. When not set, the endpoint is derived from `sqs_queue_url`. Default: `''`.
- `sqs_schema` — Schema identifier for schema-based formats such as [Cap'n Proto](https://capnproto.org/) (`schema.capnp:Message`). Default: `''`.
- `sqs_num_consumers` — Number of consumer threads per table. Increase for higher throughput. Default: `1`.
- `sqs_max_block_size` — Number of rows collected before flushing data from SQS. Default: [max_insert_block_size](/operations/settings/settings#max_insert_block_size).
- `sqs_flush_interval_ms` — Timeout for flushing data from SQS. Default: [stream_flush_interval_ms](/operations/settings/settings#stream_flush_interval_ms).
- `sqs_poll_timeout_ms` — Timeout in milliseconds between polling attempts when no messages are available. Default: `500`.
- `sqs_max_messages_per_receive` — Maximum number of messages per `ReceiveMessage` API call. AWS allows 1–10. Default: `10`.
- `sqs_visibility_timeout` — Visibility timeout in seconds: how long a received message is hidden from other consumers during processing. Default: `30`.
- `sqs_wait_time_seconds` — Long-polling wait time in seconds (0 disables long polling, maximum 20). Default: `0`.
- `sqs_auto_delete` — Automatically delete messages from the queue after successful processing. Default: `true`.
- `sqs_dead_letter_queue_url` — URL of a Dead Letter Queue. Messages that fail to parse are forwarded here instead of being skipped or causing an exception. Default: `''`.
- `sqs_max_receive_count` — Maximum number of times a message may be received before being moved to the DLQ (requires `sqs_dead_letter_queue_url`). Default: `3`.
- `sqs_skip_broken_messages` — Number of broken messages to skip per block. If `sqs_skip_broken_messages = N`, the engine skips up to `N` messages that cannot be parsed. Default: `0`.
- `sqs_max_rows_per_message` — Maximum number of rows written in one SQS message for row-based formats. Default: `1`.
- `sqs_verify_ssl` — Verify SSL certificate when connecting to SQS. Set to `0` for LocalStack or self-signed certificates. Default: `true`.
- `sqs_handle_error_mode` — How to handle parse errors. Possible values: `default` (throw an exception after `sqs_skip_broken_messages` broken messages), `stream` (not yet supported — use `sqs_skip_broken_messages` or `sqs_dead_letter_queue_url` instead). Default: `default`.

## Description {#description}

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1. Use the engine to create an SQS consumer and consider it a data stream.
2. Create a table with the desired structure.
3. Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from SQS and convert them to the required format using `SELECT`.

Example:

```sql
SET allow_experimental_sqs_table = 1;

CREATE TABLE sqs_source (
    id     UInt64,
    name   String,
    ts     DateTime
) ENGINE = SQS SETTINGS
    sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
    sqs_format = 'JSONEachRow',
    sqs_num_consumers = 4;

CREATE TABLE events (
    id     UInt64,
    name   String,
    ts     DateTime
) ENGINE = MergeTree() ORDER BY id;

CREATE MATERIALIZED VIEW consumer TO events
    AS SELECT id, name, ts FROM sqs_source;

SELECT id, name, ts FROM events ORDER BY ts;
```

Do not use the same table for inserts and materialized views.

To improve performance, received messages are grouped into blocks of size [max_insert_block_size](/operations/settings/settings#max_insert_block_size). If the block was not formed within [stream_flush_interval_ms](/operations/settings/settings#stream_flush_interval_ms) milliseconds, the data will be flushed to the table regardless of the completeness of the block.

## AWS credentials {#aws-credentials}

The engine supports two authentication modes:

- **Explicit credentials** — set both `sqs_aws_access_key_id` and `sqs_aws_secret_access_key`. Suitable for development and LocalStack.
- **Default credential provider chain** — leave both settings empty. The engine then uses the standard AWS credential resolution order: IAM instance profile, ECS/EKS task role, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` environment variables, `~/.aws/credentials` shared file. This is the recommended approach for production deployments on AWS.

Example using IAM (no keys in the table definition):

```sql
CREATE TABLE sqs_source (message String)
ENGINE = SQS SETTINGS
    sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
    sqs_format = 'JSONEachRow';
```

Example using explicit keys (LocalStack):

```sql
CREATE TABLE sqs_source (message String)
ENGINE = SQS SETTINGS
    sqs_queue_url        = 'http://localhost:4566/000000000000/my-queue',
    sqs_format           = 'JSONEachRow',
    sqs_aws_access_key_id     = 'test',
    sqs_aws_secret_access_key = 'test',
    sqs_aws_region       = 'us-east-1',
    sqs_endpoint         = 'http://localhost:4566',
    sqs_verify_ssl       = 0;
```

## Dead Letter Queue {#dead-letter-queue}

Set `sqs_dead_letter_queue_url` to route unprocessable messages to a separate SQS queue instead of blocking or discarding them:

```sql
CREATE TABLE sqs_source (...)
ENGINE = SQS SETTINGS
    sqs_queue_url          = 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
    sqs_format             = 'JSONEachRow',
    sqs_dead_letter_queue_url = 'https://sqs.us-east-1.amazonaws.com/123456789012/my-dlq',
    sqs_max_receive_count  = 3;
```

Messages that cannot be parsed are forwarded to the DLQ and deleted from the source queue.

## Virtual columns {#virtual-columns}

- `_message_id` — SQS message ID assigned by the service. Data type: `String`.
- `_receive_count` — Number of times this message has been received. Data type: `UInt64`.
- `_sent_timestamp` — Unix timestamp (seconds) when the message was sent. Data type: `UInt64`.
- `_message_group_id` — Message group ID (FIFO queues only). Data type: `String`.
- `_message_deduplication_id` — Message deduplication ID (FIFO queues only). Data type: `String`.
- `_sequence_number` — Sequence number (FIFO queues only). Data type: `String`.

Example:

```sql
SELECT _message_id, _receive_count, _sent_timestamp, message
FROM sqs_source
LIMIT 10;
```

## Data formats support {#data-formats-support}

The SQS engine supports all [formats](../../../interfaces/formats.md) supported in ClickHouse.
The number of rows in one SQS message depends on whether the format is row-based or block-based:

- For row-based formats the number of rows in one SQS message can be controlled by setting `sqs_max_rows_per_message`.
- For block-based formats the block cannot be divided into smaller parts, but the number of rows in one block can be controlled by the general setting [max_block_size](/operations/settings/settings#max_block_size).
