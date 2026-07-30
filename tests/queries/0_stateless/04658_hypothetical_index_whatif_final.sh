#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One granule per row, and two parts covering the same primary key range, so the
# PrimaryKeyExpand pass has something to re-include.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_whatif_final;
    CREATE TABLE t_whatif_final (a UInt64, b UInt64) ENGINE = ReplacingMergeTree ORDER BY a
    SETTINGS index_granularity = 1;
    SYSTEM STOP MERGES t_whatif_final;
    INSERT INTO t_whatif_final SELECT number, 0 FROM numbers(10);
    INSERT INTO t_whatif_final SELECT number, number FROM numbers(10);
    CREATE HYPOTHETICAL INDEX idx_b ON t_whatif_final (b) TYPE minmax GRANULARITY 1;
"

fields() { grep -E '^Baseline|^With |^  status:|^  marks:|^  reason:'; }

# b is outside the primary key, so a real FINAL read runs PrimaryKeyExpand: the single
# granule the candidate keeps in the second part re-includes its match in the first one.
echo "--- exact mode on, candidate outside the primary key ---"
$CLICKHOUSE_CLIENT -q "
    EXPLAIN WHATIF SELECT * FROM t_whatif_final FINAL WHERE b = 5
    SETTINGS use_skip_indexes_if_final_exact_mode = 1;
" | fields

echo "--- exact mode off, no expansion ---"
$CLICKHOUSE_CLIENT -q "
    EXPLAIN WHATIF SELECT * FROM t_whatif_final FINAL WHERE b = 5
    SETTINGS use_skip_indexes_if_final_exact_mode = 0;
" | fields

echo "--- skip indexes disabled under FINAL ---"
$CLICKHOUSE_CLIENT -q "
    EXPLAIN WHATIF SELECT * FROM t_whatif_final FINAL WHERE b = 5
    SETTINGS use_skip_indexes_if_final = 0;
" | fields

# c is part of the primary key, so skip indexes cannot drop a part FINAL needs and the
# engine leaves PrimaryKeyExpand off - the estimate must not widen either.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_whatif_final_pk;
    CREATE TABLE t_whatif_final_pk (a UInt64, c UInt64) ENGINE = ReplacingMergeTree ORDER BY (a, c)
    SETTINGS index_granularity = 1;
    SYSTEM STOP MERGES t_whatif_final_pk;
    INSERT INTO t_whatif_final_pk SELECT number, 0 FROM numbers(10);
    INSERT INTO t_whatif_final_pk SELECT number, number FROM numbers(10);
    CREATE HYPOTHETICAL INDEX idx_c ON t_whatif_final_pk (c) TYPE minmax GRANULARITY 1;
"

echo "--- exact mode on, candidate inside the primary key ---"
$CLICKHOUSE_CLIENT -q "
    EXPLAIN WHATIF SELECT * FROM t_whatif_final_pk FINAL WHERE c = 5
    SETTINGS use_skip_indexes_if_final_exact_mode = 1;
" | fields

# Without a final mark PrimaryKeyExpand gives up and reads whole parts, which we cannot
# report as pruning.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_whatif_final_nomark;
    CREATE TABLE t_whatif_final_nomark (a UInt64, b UInt64) ENGINE = ReplacingMergeTree ORDER BY a
    SETTINGS index_granularity = 1, index_granularity_bytes = 0;
    SYSTEM STOP MERGES t_whatif_final_nomark;
    INSERT INTO t_whatif_final_nomark SELECT number, 0 FROM numbers(10);
    INSERT INTO t_whatif_final_nomark SELECT number, number FROM numbers(10);
    CREATE HYPOTHETICAL INDEX idx_b ON t_whatif_final_nomark (b) TYPE minmax GRANULARITY 1;
"

echo "--- parts without a final mark ---"
$CLICKHOUSE_CLIENT -q "
    EXPLAIN WHATIF SELECT * FROM t_whatif_final_nomark FINAL WHERE b = 5
    SETTINGS use_skip_indexes_if_final_exact_mode = 1;
" | fields

echo "--- no FINAL, same table and candidate ---"
$CLICKHOUSE_CLIENT -q "
    EXPLAIN WHATIF SELECT * FROM t_whatif_final WHERE b = 5;
" | fields

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_whatif_final;
    DROP TABLE t_whatif_final_pk;
    DROP TABLE t_whatif_final_nomark;
"
