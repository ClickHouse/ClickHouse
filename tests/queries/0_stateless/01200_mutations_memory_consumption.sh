#!/usr/bin/env bash
# Tags: no-debug, long, no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This test asserts that the peak memory of a `MutatePart` operation stays below the
# number of bytes it reads (i.e. memory is almost constant and does not grow with the
# data). The server-level `additional_memory_tracking_per_thread` (4 MiB by default)
# speculatively reserves memory for every pipeline-executor thread of the mutation,
# which inflates `peak_memory_usage` in `system.part_log` by tens of MiB and breaks the
# invariant. The reservation is unrelated to what this test measures, so run inside
# `clickhouse-local` with the feature disabled. `clickhouse-local` exercises the same
# MergeTree mutation code path, with `mutations_sync = 1` making the mutation synchronous.
CONFIG_FILE=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 01200_config.XXXXXX.xml)
trap 'rm -f "$CONFIG_FILE"' EXIT
cat > "$CONFIG_FILE" <<'EOF'
<clickhouse>
    <additional_memory_tracking_per_thread>0</additional_memory_tracking_per_thread>
    <part_log>
        <database>system</database>
        <table>part_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </part_log>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" --multiquery "
SET optimize_trivial_insert_select = 1;

DROP TABLE IF EXISTS table_with_single_pk;

CREATE TABLE table_with_single_pk
(
  key UInt8,
  value String
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_compress_block_size=65536, max_compress_block_size=65536;

INSERT INTO table_with_single_pk SELECT number, toString(number % 10) FROM numbers(10000000);

ALTER TABLE table_with_single_pk DELETE WHERE key % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS part_log;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
  arrayDistinct(groupArray(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
FROM
    system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND event_type = 'MutatePart' AND table = 'table_with_single_pk' AND database = currentDatabase();

DROP TABLE IF EXISTS table_with_single_pk;

DROP TABLE IF EXISTS table_with_multi_pk;

CREATE TABLE table_with_multi_pk
(
  key1 UInt8,
  key2 UInt32,
  key3 DateTime64(6, 'UTC'),
  value String
)
ENGINE = MergeTree
ORDER BY (key1, key2, key3)
SETTINGS min_compress_block_size=65536, max_compress_block_size=65536;

INSERT INTO table_with_multi_pk SELECT number % 32, number, toDateTime('2019-10-01 00:00:00'), toString(number % 10) FROM numbers(10000000);

ALTER TABLE table_with_multi_pk DELETE WHERE key1 % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS part_log;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
  arrayDistinct(groupArray(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
  FROM
      system.part_log
 WHERE event_date >= yesterday() AND event_time >= now() - 600 AND event_type = 'MutatePart' AND table = 'table_with_multi_pk' AND database = currentDatabase();

DROP TABLE IF EXISTS table_with_multi_pk;

DROP TABLE IF EXISTS table_with_function_pk;

CREATE TABLE table_with_function_pk
  (
    key1 UInt8,
    key2 UInt32,
    key3 DateTime64(6, 'UTC'),
    value String
  )
ENGINE = MergeTree
ORDER BY (cast(value as UInt64), key2)
SETTINGS min_compress_block_size=65536, max_compress_block_size=65536;

INSERT INTO table_with_function_pk SELECT number % 32, number, toDateTime('2019-10-01 00:00:00'), toString(number % 10) FROM numbers(10000000);

ALTER TABLE table_with_function_pk DELETE WHERE key1 % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS part_log;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
  arrayDistinct(groupArray(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
  FROM
      system.part_log
 WHERE event_date >= yesterday() AND event_time >= now() - 600 AND event_type = 'MutatePart' AND table = 'table_with_function_pk' AND database = currentDatabase();

DROP TABLE IF EXISTS table_with_function_pk;

DROP TABLE IF EXISTS table_without_pk;

CREATE TABLE table_without_pk
(
  key1 UInt8,
  key2 UInt32,
  key3 DateTime64(6, 'UTC'),
  value String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_compress_block_size=65536, max_compress_block_size=65536;

INSERT INTO table_without_pk SELECT number % 32, number, toDateTime('2019-10-01 00:00:00'), toString(number % 10) FROM numbers(10000000);

ALTER TABLE table_without_pk DELETE WHERE key1 % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS part_log;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
  arrayDistinct(groupArray(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
  FROM
      system.part_log
 WHERE event_date >= yesterday() AND event_time >= now() - 600 AND event_type = 'MutatePart' AND table = 'table_without_pk' AND database = currentDatabase();

DROP TABLE IF EXISTS table_without_pk;
"
