#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two partitions of 100 granules each. A query pinned to one partition can read at most
# 100 marks, so that is what the sampled/total pair must be measured against
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_counters;
    CREATE TABLE t_hypo_counters (p UInt8, a UInt64, b UInt64)
    ENGINE = MergeTree PARTITION BY p ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0;
    INSERT INTO t_hypo_counters SELECT 0, number, number % 100 FROM numbers(10000);
    INSERT INTO t_hypo_counters SELECT 1, number, number % 100 FROM numbers(10000);
"

echo "--- totals are the marks the query could read, not the whole table ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_counters (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_counters WHERE p = 1 AND b = 42;
" 2>&1 | grep -E '^\s+sampled_(parts|marks):' | tr -s ' '

echo "--- unpartitioned query sees both partitions ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_counters (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_counters WHERE b = 42;
" 2>&1 | grep -E '^\s+sampled_(parts|marks):' | tr -s ' '

echo "--- a disabled empirical tier now says why ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_counters (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_counters WHERE b = 42 SETTINGS merge_tree_min_rows_for_seek = 1;
" 2>&1 | grep -E '^\s+(source|empirical_status|empirical_reason):' | tr -s ' ' | cut -c1-60

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_counters;"
