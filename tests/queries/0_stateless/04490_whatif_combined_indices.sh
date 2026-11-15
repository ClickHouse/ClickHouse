#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped and not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_combined;
    CREATE TABLE t_combined (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    -- b is clustered (minmax prunes it); c is a scattered bijection (bloom_filter prunes it);
    -- d cycles through every granule (no index prunes it).
    INSERT INTO t_combined SELECT number, intDiv(number, 100), (number * 7919) % 10000, number % 7 FROM numbers(10000);
"

# we can avoid dropping hypothetical indices because they are session-scoped anyway
echo "--- two hyp indexes: independent rows + combined row ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_combined (b) TYPE minmax GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX idx_c ON t_combined (c) TYPE bloom_filter GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_combined WHERE b = 42 AND c = 5000;
" | grep -E '^With |^\(combined|^\s+marks:|^\s+skip_ratio:'

echo "--- three hyp indexes: independent rows + combined row ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_combined (b) TYPE minmax GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX idx_c ON t_combined (c) TYPE bloom_filter GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX idx_d ON t_combined (d) TYPE set(10) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_combined WHERE b = 42 AND c = 5000 AND d = 3;
" | grep -E '^With |^\(combined|^\s+marks:|^\s+skip_ratio:'

echo "--- redundant hyp indexes (same column): combined equals each single ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_combined (b) TYPE minmax GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX idx_b_set ON t_combined (b) TYPE set(200) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_combined WHERE b = 42;
" | grep -E '^With |^\(combined|^\s+marks:|^\s+skip_ratio:'

echo "--- single hyp index: no combined block ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_combined (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_combined WHERE b = 42;
" | grep -cE '^\(combined'

echo "--- combined name cannot be forced (INDEX_NOT_USED) ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_combined (b) TYPE minmax GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX idx_c ON t_combined (c) TYPE bloom_filter GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_combined WHERE b = 42 AND c = 5000 SETTINGS force_data_skipping_indices = '\'(combined: idx_b, idx_c)\'';
" 2>&1 | grep -m1 -o 'INDEX_NOT_USED'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_combined;"
