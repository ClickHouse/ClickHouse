#!/usr/bin/env bash
# Tags: long, no-tsan, no-debug, no-asan, no-msan, no-flaky-check
# Random settings limits: index_granularity=(8192, None)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_compact_parts_read_lc"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_compact_parts_read_lc
(
    event_date Date CODEC(Delta(2), ZSTD(1)),
    event_time DateTime CODEC(Delta(4), ZSTD(1)),
    metric LowCardinality(String) CODEC(ZSTD(1)),
)
ENGINE = MergeTree
ORDER BY (metric, event_date, event_time)
SETTINGS min_bytes_for_wide_part = '100G'"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_compact_parts_read_lc
    SELECT event_date, event_time, toString(rand() % 1000) || repeat('a', 20) FROM
    generateRandom('event_date Date, event_time DateTime', 10, 5, 5) LIMIT 10000000
    SETTINGS max_rows_to_read = 0"

$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_compact_parts_read_lc FINAL"

for _ in {0..50}; do
    $CLICKHOUSE_CLIENT --max_threads=3 -q "SELECT metric FROM t_compact_parts_read_lc FORMAT Null"
done

echo "Ok"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_compact_parts_read_lc"
