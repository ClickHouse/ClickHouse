---
description: 'The ElasticsearchQueue table engine continuously polls Elasticsearch search results and streams them into ClickHouse through materialized views.'
sidebar_label: 'ElasticsearchQueue'
sidebar_position: 155
slug: /engines/table-engines/integrations/elasticsearch-queue
title: 'ElasticsearchQueue table engine'
doc_type: 'guide'
---

# `ElasticsearchQueue` table engine {#elasticsearchqueue-table-engine}

The `ElasticsearchQueue` table engine reads documents from Elasticsearch and streams them into ClickHouse tables through materialized views. It is designed for ingestion pipelines where data already exists in Elasticsearch and must be copied into ClickHouse for analytical queries.

`ElasticsearchQueue` is similar to other queue engines such as `Kafka` and `S3Queue`: the engine table is a source, not the final storage. A `MATERIALIZED VIEW` receives blocks from the engine table and writes them into a destination table such as `MergeTree`.

`ElasticsearchQueue` is experimental. Enable `allow_experimental_elasticsearch_queue` before creating a table:

```sql
SET allow_experimental_elasticsearch_queue = 1;
```

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]queue_table
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ElasticsearchQueue(elasticsearch_url, elasticsearch_index, elasticsearch_cursor_field)
SETTINGS
    [elasticsearch_tiebreaker_field = '',]
    [elasticsearch_query = '',]
    [elasticsearch_auth_type = 'auto',]
    [elasticsearch_user = '',]
    [elasticsearch_password = '',]
    [elasticsearch_api_key = '',]
    [elasticsearch_bearer_token = '',]
    [elasticsearch_use_point_in_time = 0,]
    [elasticsearch_pit_keep_alive = '1m',]
    [elasticsearch_keeper_path = '',]
    [elasticsearch_keeper_checkpoint_name = 'checkpoint',]
    [elasticsearch_max_block_size = 0,]
    [elasticsearch_poll_max_batch_size = 0,]
    [elasticsearch_poll_timeout_ms = 0,]
    [elasticsearch_flush_interval_ms = 0,]
    [elasticsearch_consumer_reschedule_ms = 500,]
    [elasticsearch_commit_on_select = 0];
```

Required parameters:

- `elasticsearch_url` - The base URL of the Elasticsearch cluster, for example `http://elasticsearch:9200`.
- `elasticsearch_index` - The Elasticsearch index or index pattern to read.
- `elasticsearch_cursor_field` - A sortable field used as the ascending `search_after` cursor.

The same values can also be specified through settings as `elasticsearch_url`, `elasticsearch_index`, and `elasticsearch_cursor_field`.

## Description {#description}

`ElasticsearchQueue` polls Elasticsearch with `_search` requests. Each request is sorted by `elasticsearch_cursor_field` and optionally by `elasticsearch_tiebreaker_field`. The `sort` value from the last document in a successfully inserted batch is saved as the checkpoint and used as `search_after` in the next request.

When a `MATERIALIZED VIEW` is attached, the engine starts collecting data in the background:

1. It loads the last committed `search_after` checkpoint.
2. It sends a `_search` request to Elasticsearch.
3. It converts values from `_source` into ClickHouse columns.
4. It writes the block through the attached `MATERIALIZED VIEW`.
5. It commits the new checkpoint only after the insert pipeline finishes.

This means `ElasticsearchQueue` is a forward ingestion engine. Updates or deletes in Elasticsearch are not replayed into ClickHouse after the cursor has already moved past the document. To ingest changes, use a cursor field that advances when the document changes, for example an `updated_at` timestamp.

## Example {#example}

Assume Elasticsearch contains documents like this:

```bash
curl -X PUT 'http://localhost:9200/events' \
    -H 'Content-Type: application/json' \
    -d '{
        "mappings": {
            "properties": {
                "seq": {"type": "long"},
                "message": {"type": "keyword"},
                "category": {"type": "keyword"}
            }
        }
    }'

curl -X POST 'http://localhost:9200/_bulk' \
    -H 'Content-Type: application/x-ndjson' \
    --data-binary @- <<'EOF'
{"index":{"_index":"events","_id":"1"}}
{"seq":1,"message":"created account","category":"audit"}
{"index":{"_index":"events","_id":"2"}}
{"seq":2,"message":"changed plan","category":"audit"}
{"index":{"_index":"events","_id":"3"}}
{"seq":3,"message":"sent invoice","category":"billing"}
EOF

curl -X POST 'http://localhost:9200/events/_refresh'
```

Create a destination table in ClickHouse:

```sql
CREATE TABLE events
(
    seq UInt64,
    message String,
    category String,
    ingested_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY seq;
```

Create the `ElasticsearchQueue` engine table:

```sql
SET allow_experimental_elasticsearch_queue = 1;

CREATE TABLE events_queue
(
    seq UInt64,
    message String,
    category String
)
ENGINE = ElasticsearchQueue('http://elasticsearch:9200', 'events', 'seq')
SETTINGS
    elasticsearch_poll_max_batch_size = 1000,
    elasticsearch_use_point_in_time = 1,
    elasticsearch_pit_keep_alive = '1m';
```

Attach a `MATERIALIZED VIEW`:

```sql
CREATE MATERIALIZED VIEW events_queue_mv TO events AS
SELECT
    seq,
    message,
    category
FROM events_queue;
```

Query the destination table:

```sql
SELECT seq, message, category
FROM events
ORDER BY seq;
```

Result:

```text
seq  message          category
1    created account  audit
2    changed plan     audit
3    sent invoice     billing
```

## Point-in-time search {#point-in-time-search}

Set `elasticsearch_use_point_in_time = 1` to use Elasticsearch point-in-time search for every poll:

```sql
CREATE TABLE events_queue
(
    seq UInt64,
    message String,
    category String
)
ENGINE = ElasticsearchQueue('http://elasticsearch:9200', 'events', 'seq')
SETTINGS
    elasticsearch_use_point_in_time = 1,
    elasticsearch_pit_keep_alive = '30s';
```

For each poll, `ElasticsearchQueue` opens a PIT with `POST /<index>/_pit`, uses the returned PIT `id` in `_search`, and closes it with `DELETE /_pit` when the request finishes.

`PIT` makes each poll observe a consistent Elasticsearch search context. It does not make `ElasticsearchQueue` re-read old documents whose cursor value is already behind the committed checkpoint.

## Replicated ingestion with Keeper {#replicated-ingestion-with-keeper}

By default, `ElasticsearchQueue` stores the checkpoint under the local table data path. In replicated deployments, configure `elasticsearch_keeper_path` so all replicas share one checkpoint in Keeper:

```sql
CREATE TABLE events_queue
(
    seq UInt64,
    message String,
    category String
)
ENGINE = ElasticsearchQueue('http://elasticsearch:9200', 'events', 'seq')
SETTINGS
    elasticsearch_keeper_path = '/clickhouse/elasticsearch_queue/events',
    elasticsearch_keeper_checkpoint_name = 'checkpoint';
```

When `elasticsearch_keeper_path` is configured:

- The checkpoint is stored at `<elasticsearch_keeper_path>/<elasticsearch_keeper_checkpoint_name>`.
- An ephemeral lock at `<elasticsearch_keeper_path>/lock` serializes polling across replicas.
- A replica reloads the shared checkpoint after it acquires the lock.
- The checkpoint is committed only while the same Keeper session still owns the lock.

`elasticsearch_keeper_checkpoint_name` defaults to `checkpoint`. It cannot be `lock`.

## Authentication {#authentication}

`ElasticsearchQueue` supports these authentication modes through `elasticsearch_auth_type`:

| Value | Required settings | HTTP behavior |
|---|---|---|
| `auto` | One credential family or none | Infers `api_key`, then `bearer`, then `basic`, then no authentication |
| `none` | No credentials | Sends no authentication |
| `basic` | `elasticsearch_user` | Sends HTTP basic authentication with `elasticsearch_user` and `elasticsearch_password` |
| `api_key` | `elasticsearch_api_key` | Sends `Authorization: ApiKey ...` |
| `bearer` | `elasticsearch_bearer_token` | Sends `Authorization: Bearer ...` |

Example with API key authentication:

```sql
CREATE TABLE events_queue
(
    seq UInt64,
    message String,
    category String
)
ENGINE = ElasticsearchQueue('https://elasticsearch.example.com:9200', 'events', 'seq')
SETTINGS
    elasticsearch_auth_type = 'api_key',
    elasticsearch_api_key = 'base64_api_key';
```

`elasticsearch_auth_type = 'auto'` rejects mixed credential families. For example, setting both `elasticsearch_api_key` and `elasticsearch_bearer_token` is rejected.

`elasticsearch_password`, `elasticsearch_api_key`, and `elasticsearch_bearer_token` are hidden in formatted `CREATE TABLE` output.

## Named collections {#named-collections}

`ElasticsearchQueue` can read connection settings from named collections.

Example configuration:

```xml
<clickhouse>
    <named_collections>
        <events_elasticsearch>
            <url>http://elasticsearch:9200</url>
            <index>events</index>
            <cursor_field>seq</cursor_field>
            <auth_type>api_key</auth_type>
            <api_key>base64_api_key</api_key>
        </events_elasticsearch>
    </named_collections>
</clickhouse>
```

Create a queue table from the named collection:

```sql
CREATE TABLE events_queue
(
    seq UInt64,
    message String,
    category String
)
ENGINE = ElasticsearchQueue(events_elasticsearch)
SETTINGS
    elasticsearch_use_point_in_time = 1;
```

Supported aliases include `url`, `index`, `cursor_field`, `user`, `password`, `api_key`, `auth_type`, `bearer_token`, `use_point_in_time`, `pit_keep_alive`, `keeper_path`, and `keeper_checkpoint_name`.

## Virtual columns {#virtual-columns}

`ElasticsearchQueue` exposes these virtual columns:

| Column | Type | Description |
|---|---|---|
| `_id` | `String` | Elasticsearch document `_id`. |
| `_index` | `LowCardinality(String)` | Source index name. |
| `_score` | `Nullable(Float64)` | Elasticsearch `_score`. |
| `_source` | `String` | Raw `_source` object serialized as JSON. |

Example:

```sql
CREATE MATERIALIZED VIEW events_queue_mv TO events_raw AS
SELECT
    _id,
    _index,
    _source
FROM events_queue;
```

## Settings {#settings}

| Setting | Default | Description |
|---|---:|---|
| `elasticsearch_url` | `''` | Base URL of Elasticsearch. |
| `elasticsearch_index` | `''` | Index or index pattern. |
| `elasticsearch_cursor_field` | `''` | Ascending field used for `search_after`. |
| `elasticsearch_tiebreaker_field` | `''` | Optional second ascending sort field for deterministic ordering. |
| `elasticsearch_query` | `''` | Optional Elasticsearch JSON request body. The engine overwrites `size`, `sort`, `pit`, and `search_after`. |
| `elasticsearch_auth_type` | `'auto'` | Authentication mode. Supported values are `auto`, `none`, `basic`, `api_key`, and `bearer`. |
| `elasticsearch_user` | `''` | User for HTTP basic authentication. |
| `elasticsearch_password` | `''` | Password for HTTP basic authentication. |
| `elasticsearch_api_key` | `''` | API key for `Authorization: ApiKey`. |
| `elasticsearch_bearer_token` | `''` | Bearer token for `Authorization: Bearer`. |
| `elasticsearch_use_point_in_time` | `0` | Enables PIT lifecycle per poll. |
| `elasticsearch_pit_keep_alive` | `'1m'` | PIT keep-alive value. |
| `elasticsearch_keeper_path` | `''` | Keeper path for a shared replicated checkpoint. |
| `elasticsearch_keeper_checkpoint_name` | `'checkpoint'` | Keeper node name for the checkpoint value. |
| `elasticsearch_max_block_size` | `0` | Maximum number of rows flushed into ClickHouse. Uses `max_insert_block_size` when unset. |
| `elasticsearch_poll_max_batch_size` | `0` | Maximum number of Elasticsearch hits requested per poll. Uses `max_block_size` when unset. |
| `elasticsearch_poll_timeout_ms` | `0` | Poll timeout. Uses `stream_poll_timeout_ms` when unset. |
| `elasticsearch_flush_interval_ms` | `0` | Maximum background work interval. Uses `stream_flush_interval_ms` when unset. |
| `elasticsearch_consumer_reschedule_ms` | `500` | Delay before the background consumer is scheduled again after a stalled poll. |
| `elasticsearch_commit_on_select` | `0` | Commits the checkpoint after a direct `SELECT` from the queue table. |

## Limitations {#limitations}

- `ElasticsearchQueue` is an ingestion engine, not a general-purpose Elasticsearch query engine.
- Direct `SELECT` is rejected while materialized views are attached.
- `elasticsearch_cursor_field` must be sortable and monotonic for the intended ingestion order.
- Updates or deletes in Elasticsearch after a document was already consumed are not replayed into ClickHouse.
- The engine does not deduplicate rows written to the destination table. Use an appropriate destination table design if idempotency is required.
