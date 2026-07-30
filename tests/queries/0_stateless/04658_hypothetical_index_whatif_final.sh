#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped and not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hypothetical indexes are session-scoped, so each case defines its own and runs EXPLAIN WHATIF
# in the same client invocation.
whatif() {
    $CLICKHOUSE_CLIENT -q "$1" | grep -E '^Baseline|^With |^  status:|^  marks:|^  reason:'
}

# One granule per row, and two parts covering the same primary key range, so the
# PrimaryKeyExpand pass has something to re-include.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_whatif_final;
    CREATE TABLE t_whatif_final (a UInt64, b UInt64) ENGINE = ReplacingMergeTree ORDER BY a
    SETTINGS index_granularity = 1;
    SYSTEM STOP MERGES t_whatif_final;
    INSERT INTO t_whatif_final SELECT number, 0 FROM numbers(10);
    INSERT INTO t_whatif_final SELECT number, number FROM numbers(10);
"

# b is outside the primary key, so a real FINAL read runs PrimaryKeyExpand and the one granule
# the candidate keeps drags its primary key neighbours back in - far more than the 1 mark the
# same candidate reaches without FINAL.
echo "--- exact mode on, candidate outside the primary key ---"
whatif "
    CREATE HYPOTHETICAL INDEX idx_b ON t_whatif_final (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_whatif_final FINAL WHERE b = 5
    SETTINGS use_skip_indexes_if_final_exact_mode = 1;
"

echo "--- exact mode off, no expansion ---"
whatif "
    CREATE HYPOTHETICAL INDEX idx_b ON t_whatif_final (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_whatif_final FINAL WHERE b = 5
    SETTINGS use_skip_indexes_if_final_exact_mode = 0;
"

echo "--- skip indexes disabled under FINAL ---"
whatif "
    CREATE HYPOTHETICAL INDEX idx_b ON t_whatif_final (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_whatif_final FINAL WHERE b = 5
    SETTINGS use_skip_indexes_if_final = 0;
"

echo "--- no FINAL, same table and candidate ---"
whatif "
    CREATE HYPOTHETICAL INDEX idx_b ON t_whatif_final (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_whatif_final WHERE b = 5;
"

# c is part of the primary key, so skip indexes cannot drop a part FINAL needs and the engine
# leaves PrimaryKeyExpand off - the estimate must not widen either.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_whatif_final_pk;
    CREATE TABLE t_whatif_final_pk (a UInt64, c UInt64) ENGINE = ReplacingMergeTree ORDER BY (a, c)
    SETTINGS index_granularity = 1;
    SYSTEM STOP MERGES t_whatif_final_pk;
    INSERT INTO t_whatif_final_pk SELECT number, 0 FROM numbers(10);
    INSERT INTO t_whatif_final_pk SELECT number, number FROM numbers(10);
"

echo "--- exact mode on, candidate inside the primary key ---"
whatif "
    CREATE HYPOTHETICAL INDEX idx_c ON t_whatif_final_pk (c) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_whatif_final_pk FINAL WHERE c = 5
    SETTINGS use_skip_indexes_if_final_exact_mode = 1;
"

# Without a final mark PrimaryKeyExpand gives up and reads whole parts.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_whatif_final_nomark;
    CREATE TABLE t_whatif_final_nomark (a UInt64, b UInt64) ENGINE = ReplacingMergeTree ORDER BY a
    SETTINGS index_granularity = 1, index_granularity_bytes = 0;
    SYSTEM STOP MERGES t_whatif_final_nomark;
    INSERT INTO t_whatif_final_nomark SELECT number, 0 FROM numbers(10);
    INSERT INTO t_whatif_final_nomark SELECT number, number FROM numbers(10);
"

# The baseline is every granule anyway, so the honest answer is that the candidate buys nothing.
echo "--- no final mark, baseline is the whole table ---"
whatif "
    CREATE HYPOTHETICAL INDEX idx_b ON t_whatif_final_nomark (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_whatif_final_nomark FINAL WHERE b = 5
    SETTINGS use_skip_indexes_if_final_exact_mode = 1;
"

# Here the primary key narrows the baseline first, so the whole-part fallback would read more
# than the query does today - that cannot be reported as pruning.
echo "--- no final mark, expansion wider than the baseline ---"
whatif "
    CREATE HYPOTHETICAL INDEX idx_b ON t_whatif_final_nomark (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_whatif_final_nomark FINAL WHERE a > 2 AND b = 5
    SETTINGS use_skip_indexes_if_final_exact_mode = 1;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_whatif_final;
    DROP TABLE t_whatif_final_pk;
    DROP TABLE t_whatif_final_nomark;
"
