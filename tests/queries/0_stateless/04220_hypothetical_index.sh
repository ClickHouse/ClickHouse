#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped and not replicated

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo;
    CREATE TABLE t_hypo (a UInt64, b String, c Float64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo SELECT number, toString(number), number * 1.1 FROM numbers(1000);
"

# All hypothetical index operations must be in a single session since they are session-scoped.

echo "--- CREATE / system table ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo (b) TYPE set(100) GRANULARITY 1;
    SELECT name, table, type, granularity FROM system.hypothetical_indexes;
"

echo "--- Duplicate errors ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo (b) TYPE set(100) GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo (b) TYPE set(100) GRANULARITY 1;
" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'

echo "--- IF NOT EXISTS is silent ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo (b) TYPE set(100) GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX IF NOT EXISTS idx_b ON t_hypo (b) TYPE set(100) GRANULARITY 1;
"

echo "--- Multiple indexes ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo (b) TYPE set(100) GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX idx_c ON t_hypo (c) TYPE minmax GRANULARITY 2;
    SELECT name, type, granularity FROM system.hypothetical_indexes ORDER BY name;
"

echo "--- Drop one ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo (b) TYPE set(100) GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX idx_c ON t_hypo (c) TYPE minmax GRANULARITY 2;
    DROP HYPOTHETICAL INDEX idx_b ON t_hypo;
    SELECT name FROM system.hypothetical_indexes;
"

echo "--- Drop non-existent errors ---"
$CLICKHOUSE_CLIENT -q "DROP HYPOTHETICAL INDEX idx_nonexistent ON t_hypo" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'

echo "--- IF EXISTS is silent ---"
$CLICKHOUSE_CLIENT -q "DROP HYPOTHETICAL INDEX IF EXISTS idx_nonexistent ON t_hypo"

echo "--- Drop all ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo (b) TYPE set(100) GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX idx_c ON t_hypo (c) TYPE minmax GRANULARITY 2;
    SELECT count() FROM system.hypothetical_indexes;
    DROP ALL HYPOTHETICAL INDEXES;
    SELECT count() FROM system.hypothetical_indexes;
"

echo "--- Non-MergeTree table errors ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_mem;
    CREATE TABLE t_hypo_mem (a UInt64) ENGINE = Memory;
"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL INDEX idx_a ON t_hypo_mem (a) TYPE minmax GRANULARITY 1" 2>&1 | grep -m1 -o 'NOT_IMPLEMENTED'
$CLICKHOUSE_CLIENT -q "DROP TABLE t_hypo_mem"

echo "--- EXPLAIN WHATIF: no indexes ---"
$CLICKHOUSE_CLIENT -q "EXPLAIN WHATIF SELECT * FROM t_hypo WHERE a > 500" | grep -E '^\s+status:|^\s+reason:|^\(none\)'

echo "--- EXPLAIN WHATIF: minmax applicable ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_a_minmax ON t_hypo (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo WHERE a > 500;
" | grep -E '^\s+status:|^With '

echo "--- EXPLAIN WHATIF: not applicable ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_c_minmax ON t_hypo (c) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo WHERE b = 'hello';
" | grep -E '^\s+status:|^\s+reason:|^With '

echo "--- EXPLAIN WHATIF: bloom_filter applicable ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b_bloom ON t_hypo (b) TYPE bloom_filter(0.01) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo WHERE b = 'hello';
" | grep -E '^\s+status:|^With '

echo "--- EXPLAIN WHATIF errors ---"
$CLICKHOUSE_CLIENT -q "EXPLAIN WHATIF INSERT INTO t_hypo VALUES (1, '1', 1.0)" 2>&1 | grep -m1 -o 'INCORRECT_QUERY'

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_mem;
    CREATE TABLE t_hypo_mem (a UInt64) ENGINE = Memory;
    INSERT INTO t_hypo_mem SELECT number FROM numbers(100);
"
$CLICKHOUSE_CLIENT -q "EXPLAIN WHATIF SELECT * FROM t_hypo_mem WHERE a > 50" 2>&1 | grep -m1 -o 'NOT_IMPLEMENTED'
$CLICKHOUSE_CLIENT -q "DROP TABLE t_hypo_mem"

echo "--- Session scope: real queries unaffected ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_a_set ON t_hypo (a) TYPE set(100) GRANULARITY 1;
    SELECT count() FROM t_hypo WHERE a > 500;
"

echo "--- Formatting ---"
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('DROP HYPOTHETICAL INDEX idx ON t')"
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('DROP ALL HYPOTHETICAL INDEXES')"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_hypo"
