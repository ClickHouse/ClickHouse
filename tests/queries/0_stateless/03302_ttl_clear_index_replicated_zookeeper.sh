#!/usr/bin/env bash
# Tags: long, replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ttl_clear_index_repl1"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ttl_clear_index_repl2"

$CLICKHOUSE_CLIENT --query="
CREATE TABLE ttl_clear_index_repl1
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ttl_clear_index_repl', 'r1')
PARTITION BY toYYYYMM(d)
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE ttl_clear_index_repl2
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/ttl_clear_index_repl', 'r2')
PARTITION BY toYYYYMM(d)
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"

$CLICKHOUSE_CLIENT --query="INSERT INTO ttl_clear_index_repl1 VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2)"
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA ttl_clear_index_repl2"

$CLICKHOUSE_CLIENT --query="
SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_repl2'
  AND active
"

$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE ttl_clear_index_repl1 FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 0, optimize_throw_if_noop = 0"
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA ttl_clear_index_repl2"

$CLICKHOUSE_CLIENT --query="
SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_repl2'
  AND active
"

$CLICKHOUSE_CLIENT --query="ALTER TABLE ttl_clear_index_repl1 MATERIALIZE INDEX idx SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA ttl_clear_index_repl2"

$CLICKHOUSE_CLIENT --query="
SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_repl2'
  AND active
"

$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE ttl_clear_index_repl1 FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_throw_if_noop = 1"
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA ttl_clear_index_repl2"

$CLICKHOUSE_CLIENT --query="
SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_repl2'
  AND active
"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM ttl_clear_index_repl2 WHERE v = 2"

$CLICKHOUSE_CLIENT --query="DROP TABLE ttl_clear_index_repl1"
$CLICKHOUSE_CLIENT --query="DROP TABLE ttl_clear_index_repl2"
