#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE source
(
    id String CODEC(ZSTD(1)),
    name LowCardinality(String),
    meta String CODEC(ZSTD(3)),
    props String CODEC(ZSTD(3)),
    headers String CODEC(ZSTD(3)),
    debug String CODEC(ZSTD(3)),
    ts DateTime64(6, 'UTC') CODEC(ZSTD(1)),
    insertedAt DateTime64(6, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (toDate(ts), name, id)
SETTINGS index_granularity = 8192
EOF

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE middle
(
    id String CODEC(ZSTD(1)),
    appUserId String CODEC(ZSTD(1)),
    applicationId UInt32,
    name LowCardinality(String),
    meta String CODEC(ZSTD(3)),
    props String CODEC(ZSTD(3)),
    headers String CODEC(ZSTD(3)),
    debug String CODEC(ZSTD(3)),
    isSandbox UInt8,
    ts DateTime64(6, 'UTC') CODEC(ZSTD(1)),
    insertedAt DateTime64(6, 'UTC') DEFAULT now64(),
    isDeleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(insertedAt, isDeleted)
PARTITION BY toYYYYMM(ts)
PRIMARY KEY (applicationId, isSandbox, toStartOfHour(ts), name, appUserId, id)
ORDER BY (applicationId, isSandbox, toStartOfHour(ts), name, appUserId, id, ts)
SETTINGS index_granularity = 8192
EOF

${CLICKHOUSE_CLIENT} <<EOF
CREATE MATERIALIZED VIEW source_mv TO middle
(
    id String,
    appUserId String,
    applicationId Int64,
    name LowCardinality(String),
    meta String,
    props String,
    headers String,
    debug String,
    isSandbox UInt8,
    ts DateTime64(6, 'UTC'),
    insertedAt DateTime64(6, 'UTC'),
    isDeleted UInt8
)
AS SELECT
    id,
    JSONExtractString(meta, 'appUserId') AS appUserId,
    JSONExtractInt(meta, 'applicationId') AS applicationId,
    name,
    meta,
    props,
    headers,
    debug,
    JSONExtractBool(meta, 'isSandbox') AS isSandbox,
    ts,
    insertedAt,
    0 AS isDeleted
FROM source
WHERE notEmpty(appUserId) AND (applicationId != 0) AND (ts > toDate(fromUnixTimestamp(((60 * 60) * 24) * 365)))
EOF

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE destination
(
    appUserId String CODEC(ZSTD(1)),
    key LowCardinality(String) CODEC(ZSTD(3)),
    type LowCardinality(String) CODEC(ZSTD(3)),
    value String CODEC(ZSTD(1)),
    applicationId UInt64,
    isSandbox UInt8,
    isDeleted UInt8,
    ts DateTime64(6, 'UTC'),
    jsType LowCardinality(String) MATERIALIZED multiIf(position(lower(type), 'int') != 0, 'number', position(lower(type), 'double') != 0, 'number', position(lower(type), 'string') != 0, 'string', position(lower(type), 'object') != 0, 'object', position(lower(type), 'array') != 0, 'array', position(lower(type), 'null') != 0, 'null', position(lower(type), 'bool') != 0, 'boolean', 'string')
)
ENGINE = ReplacingMergeTree(ts, isDeleted)
ORDER BY (applicationId, isSandbox, appUserId, key)
SETTINGS index_granularity = 512
EOF

${CLICKHOUSE_CLIENT} <<EOF
CREATE MATERIALIZED VIEW middle_mv TO destination
(
    appUserId String,
    key String,
    type Enum8('Null' = 0, 'String' = 34, 'Array' = 91, 'Bool' = 98, 'Double' = 100, 'Int64' = 105, 'UInt64' = 117, 'Object' = 123),
    value String,
    applicationId UInt32,
    isSandbox UInt8,
    isDeleted UInt8,
    ts DateTime64(6, 'UTC')
)
AS WITH propertyTuples AS
    (
        SELECT
            appUserId,
            arrayJoin(arrayMap(t -> (t.1, t.2, if((t.2) = 'String', JSONExtractString(t.3), t.3)), arrayMap(k -> (k, JSONType(props, k), JSONExtractRaw(props, k)), arrayFilter(t -> ((t != '\$event_name') AND (t != 'event_name') AND (t != '\$is_standard_event')), JSONExtractKeys(props))))) AS property,
            applicationId,
            isSandbox,
            ts
        FROM middle
        WHERE (isValidJSON(props) = 1) AND (name = 'device_attributes')
    )
SELECT
    appUserId,
    property.1 AS key,
    property.2 AS type,
    property.3 AS value,
    applicationId,
    isSandbox,
    0 AS isDeleted,
    ts
FROM propertyTuples
EOF

${CLICKHOUSE_CLIENT} <<EOF
INSERT INTO source ("debug", "headers", "id", "meta", "name", "props", "ts")
VALUES (
    'debug_string',
    'headers_string',
    'id_string',
    '{"appUserId":"app_user_id_string","applicationId":1234567890,"isSandbox":1}',
    'name_string',
    '{"key1":"value1","key2":"value2"}',
    now64()
)
EOF
