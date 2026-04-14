#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-fasttest

# Verify that RemoteReadingManager logs a ReadScope for every column file read,
# with correct phase (Prewhere / Rest) for different part types and query shapes.
# Verify that ReadBufferFromRRM prefetch is involved for S3-backed tables.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# -- helpers ------------------------------------------------------------------

QUERY_IDS=()
QUERY_DESCS=()
QUERY_PREFIX="${CLICKHOUSE_DATABASE}_rrm_${RANDOM}"
QUERY_SEQ=0

S3_SETTINGS="enable_filesystem_cache = 0"

function run_query()
{
    local desc="$1"
    local query="$2"
    local query_id="${QUERY_PREFIX}_$((++QUERY_SEQ))"

    QUERY_IDS+=("$query_id")
    QUERY_DESCS+=("$desc")

    $CLICKHOUSE_CLIENT --query_id="$query_id" --query "$query" >/dev/null 2>&1
}

function collect_all_scopes()
{
    for i in "${!QUERY_IDS[@]}"; do
        echo "--- ${QUERY_DESCS[$i]} ---"
        $CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
            SELECT
                extract(message, 'file=([^ ]+)') AS file,
                extract(message, 'phase=(\\\\w+)') AS phase
            FROM system.text_log
            WHERE event_date >= yesterday()
              AND event_time >= now() - 120
              AND query_id = '${QUERY_IDS[$i]}'
              AND logger_name = 'RemoteReadingManager'
              AND message LIKE '%createReadBuffer%'
            ORDER BY file, phase
        "
    done
}

function collect_rrm_prefetch()
{
    for i in "${!QUERY_IDS[@]}"; do
        echo "--- ${QUERY_DESCS[$i]} ---"
        $CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
            SELECT count() > 0 AS has_prefetch
            FROM system.text_log
            WHERE event_date >= yesterday()
              AND event_time >= now() - 120
              AND query_id = '${QUERY_IDS[$i]}'
              AND logger_name = 'ReadBufferFromRRM'
              AND message LIKE '%Prefetch complete%'
        "
    done
}

# -- setup --------------------------------------------------------------------

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_rrm_wide;
    CREATE TABLE t_rrm_wide (a UInt64, b UInt64, c UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS
        index_granularity = 128,
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        storage_policy = 's3_cache';

    INSERT INTO t_rrm_wide SELECT number, number % 10, number * 2 FROM numbers(1000);

    DROP TABLE IF EXISTS t_rrm_compact;
    CREATE TABLE t_rrm_compact (a UInt64, b UInt64, c UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS
        index_granularity = 128,
        min_bytes_for_wide_part = 1000000000,
        min_rows_for_wide_part = 1000000000,
        storage_policy = 's3_cache';

    INSERT INTO t_rrm_compact SELECT number, number % 10, number * 2 FROM numbers(1000);
"

# -- stage 1: verify data correctness -----------------------------------------
# Each query checks that data read through RRM matches expected results.

echo "=== data correctness ==="

echo "wide, no prewhere:"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(a), sum(b), sum(c) FROM t_rrm_wide
    SETTINGS optimize_move_to_prewhere = 0, $S3_SETTINGS
"

echo "wide, explicit prewhere:"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(a), sum(c) FROM t_rrm_wide PREWHERE b > 5
    SETTINGS enable_multiple_prewhere_read_steps = 1, $S3_SETTINGS
"

echo "compact, no prewhere:"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(a), sum(b), sum(c) FROM t_rrm_compact
    SETTINGS optimize_move_to_prewhere = 0, $S3_SETTINGS
"

echo "compact, explicit prewhere:"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(a), sum(c) FROM t_rrm_compact PREWHERE b > 5
    SETTINGS enable_multiple_prewhere_read_steps = 1, $S3_SETTINGS
"

echo "wide, read in order with prewhere:"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(a), sum(c) FROM (
        SELECT a, c FROM t_rrm_wide PREWHERE b > 5 ORDER BY a
        SETTINGS enable_multiple_prewhere_read_steps = 1, optimize_read_in_order = 1, $S3_SETTINGS
    )
"

# -- stage 2: run queries for log collection ----------------------------------

run_query "wide, no prewhere" "
    SELECT a, b, c FROM t_rrm_wide FORMAT Null
    SETTINGS optimize_move_to_prewhere = 0, $S3_SETTINGS
"

run_query "wide, explicit prewhere on b" "
    SELECT a, c FROM t_rrm_wide PREWHERE b > 5 FORMAT Null
    SETTINGS enable_multiple_prewhere_read_steps = 1, $S3_SETTINGS
"

run_query "wide, auto prewhere on b" "
    SELECT a, c FROM t_rrm_wide WHERE b > 5 FORMAT Null
    SETTINGS optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 1, $S3_SETTINGS
"

run_query "compact, no prewhere" "
    SELECT a, b, c FROM t_rrm_compact FORMAT Null
    SETTINGS optimize_move_to_prewhere = 0, $S3_SETTINGS
"

run_query "compact, explicit prewhere on b" "
    SELECT a, c FROM t_rrm_compact PREWHERE b > 5 FORMAT Null
    SETTINGS enable_multiple_prewhere_read_steps = 1, $S3_SETTINGS
"

run_query "wide, read in order with prewhere" "
    SELECT a, c FROM t_rrm_wide PREWHERE b > 5 ORDER BY a FORMAT Null
    SETTINGS enable_multiple_prewhere_read_steps = 1, optimize_read_in_order = 1, $S3_SETTINGS
"

# -- stage 2: flush logs, then collect results --------------------------------

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"

collect_all_scopes

echo "=== ReadBufferFromRRM prefetch ==="
collect_rrm_prefetch

# -- cleanup ------------------------------------------------------------------

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_rrm_wide;
    DROP TABLE IF EXISTS t_rrm_compact;
"
