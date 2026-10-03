#!/usr/bin/env bash

# Lazy `FINAL` with a skip index and `use_skip_indexes_if_final_exact_mode`.
# The pass that adds back the granules rejected by the skip index (`findPKRangesForFinalAfterSkipIndex`)
# is needed only by the fallback `FINAL` read. The fallback read is analyzed and built only when
# the query selects it, so a query that uses the lazy branch does not run the pass at all.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

settings="--use_skip_indexes=1 --use_skip_indexes_if_final=1 --use_skip_indexes_if_final_exact_mode=1 --use_query_condition_cache=0 --enable_parallel_replicas=0"
lazy="query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0"

$CLICKHOUSE_CLIENT $settings -m -q "
    DROP TABLE IF EXISTS t_lazy_final_skip_index;

    CREATE TABLE t_lazy_final_skip_index
    (
        key UInt64,
        ver UInt64,
        v UInt8,
        INDEX idx_v v TYPE minmax GRANULARITY 1
    )
    ENGINE = ReplacingMergeTree(ver)
    ORDER BY key
    SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

    SYSTEM STOP MERGES t_lazy_final_skip_index;

    -- Four parts with interleaved keys, so each part covers the whole key range.
    INSERT INTO t_lazy_final_skip_index SELECT number * 4 + 0, 1, intHash64(number * 4 + 0) % 509 = 0 FROM numbers(4000);
    INSERT INTO t_lazy_final_skip_index SELECT number * 4 + 1, 1, intHash64(number * 4 + 1) % 509 = 0 FROM numbers(4000);
    INSERT INTO t_lazy_final_skip_index SELECT number * 4 + 2, 1, intHash64(number * 4 + 2) % 509 = 0 FROM numbers(4000);
    INSERT INTO t_lazy_final_skip_index SELECT number * 4 + 3, 1, intHash64(number * 4 + 3) % 509 = 0 FROM numbers(4000);
    -- Newer versions: hide some old matches and add some new ones.
    INSERT INTO t_lazy_final_skip_index SELECT number, 2, intHash64(number) % 1013 = 0 FROM numbers(16000)
        WHERE intHash64(number) % 509 = 0 AND number % 2 = 0 OR intHash64(number) % 1013 = 0;
    -- A part that does not intersect the others, so lazy FINAL splits it off.
    INSERT INTO t_lazy_final_skip_index SELECT 100000 + number, 1, intHash64(number) % 101 = 0 FROM numbers(4000);
"

# Which branch the query takes, and how many times the pass that adds back the rejected granules runs.
function check_log()
{
    local log
    log=$($CLICKHOUSE_CLIENT $settings --send_logs_level=trace -q "SELECT count() FROM t_lazy_final_skip_index FINAL WHERE $1 SETTINGS $lazy, max_rows_for_lazy_final = $2" 2>&1 >/dev/null)
    echo "$log" | grep -o 'Lazy FINAL [a-z]*abled' | head -1
    echo "findPKRangesForFinalAfterSkipIndex: $(echo "$log" | grep -c 'findPKRangesForFinalAfterSkipIndex')"
}

function check()
{
    echo "-- expected result"
    $CLICKHOUSE_CLIENT $settings -q "SELECT count(), sum(key) FROM t_lazy_final_skip_index FINAL WHERE $1 SETTINGS use_skip_indexes = 0, query_plan_optimize_lazy_final = 0"

    echo "-- lazy branch"
    $CLICKHOUSE_CLIENT $settings -q "SELECT count(), sum(key) FROM t_lazy_final_skip_index FINAL WHERE $1 SETTINGS $lazy, max_rows_for_lazy_final = 1000000"

    echo "-- fallback branch"
    $CLICKHOUSE_CLIENT $settings -q "SELECT count(), sum(key) FROM t_lazy_final_skip_index FINAL WHERE $1 SETTINGS $lazy, max_rows_for_lazy_final = 1"

    echo "-- lazy branch log"
    check_log "$1" 1000000

    echo "-- fallback branch log"
    check_log "$1" 1
}

echo "=== with the non-intersecting part"
check "v = 1"

echo "=== without the non-intersecting part"
check "v = 1 AND key < 50000"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_final_skip_index"
