#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# All hypothetical-index state is session-scoped, so each scenario uses
# a single $CLICKHOUSE_CLIENT invocation that sets up and asserts what it needs

# =========================================================
# Empty system.hypothetical_indexes — a fresh session has none
# =========================================================
echo "--- empty system table ---"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.hypothetical_indexes"

# =========================================================
# Drop and recreate the table — the old hypothetical index must NOT
# apply to the new table (the store keys on UUID)
# =========================================================
echo "--- drop/recreate: old index applies before drop ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_lc;
    CREATE TABLE t_hypo_lc (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_lc SELECT number, number FROM numbers(100);

    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_lc (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_lc WHERE b = 42;

    SELECT '--- drop/recreate: new table does NOT see old index ---';
    DROP TABLE t_hypo_lc;
    CREATE TABLE t_hypo_lc (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_lc SELECT number, number FROM numbers(100);
    EXPLAIN WHATIF SELECT * FROM t_hypo_lc WHERE b = 42;
" | grep -E '^---|^With idx_b|^\(none\):'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_lc"

# =========================================================
# After DROP/CREATE on the same name, a stale entry from the old UUID
# must still be removable via DROP HYPOTHETICAL INDEX, and re-creating
# the index on the new table must purge it so it doesn't pile up
# =========================================================
echo "--- drop/recreate: stale entry is removable by name ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_stale;
    CREATE TABLE t_hypo_stale (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_stale (b) TYPE minmax GRANULARITY 1;
    DROP TABLE t_hypo_stale;
    CREATE TABLE t_hypo_stale (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_stale';
    DROP HYPOTHETICAL INDEX idx_b ON t_hypo_stale;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_stale';
" | grep -E '^[0-9]+$'

echo "--- drop/recreate: re-creating the index purges the stale entry ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_stale;
    CREATE TABLE t_hypo_stale (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_stale (b) TYPE minmax GRANULARITY 1;
    DROP TABLE t_hypo_stale;
    CREATE TABLE t_hypo_stale (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_stale (b) TYPE minmax GRANULARITY 1;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_stale';
" | grep -E '^[0-9]+$'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_stale"

# RENAME + reuse: don't touch the entry for the renamed table
echo "--- rename + reuse: DROP on the new table does not touch the renamed one ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_rn;
    DROP TABLE IF EXISTS t_hypo_rn2;
    CREATE TABLE t_hypo_rn (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_rn (b) TYPE minmax GRANULARITY 1;
    RENAME TABLE t_hypo_rn TO t_hypo_rn2;
    CREATE TABLE t_hypo_rn (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    DROP HYPOTHETICAL INDEX idx_b ON t_hypo_rn;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_rn';
    DROP TABLE t_hypo_rn;
    DROP TABLE t_hypo_rn2;
" 2>&1 | grep -oE 'BAD_ARGUMENTS|^[0-9]+$'

# =========================================================
# IF NOT EXISTS is silent on duplicate; second CREATE with no IF errors
# =========================================================
echo "--- IF NOT EXISTS is silent on duplicate ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_dup;
    CREATE TABLE t_hypo_dup (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_a ON t_hypo_dup (a) TYPE minmax GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX IF NOT EXISTS idx_a ON t_hypo_dup (a) TYPE minmax GRANULARITY 1;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_dup';

    SELECT '--- DROP IF EXISTS is silent on missing ---';
    DROP HYPOTHETICAL INDEX IF EXISTS idx_nope ON t_hypo_dup;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_dup';
" | grep -E '^---|^[0-9]+$'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_dup"

# =========================================================
# Applicability: set index on column b, predicate on column c → not applicable
# =========================================================
echo "--- applicability: predicate doesn't reference index column ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_app;
    CREATE TABLE t_hypo_app (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_app SELECT number, number, toString(number) FROM numbers(1000);

    CREATE HYPOTHETICAL INDEX idx_b_set ON t_hypo_app (b) TYPE set(100) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_app WHERE c = 'foo';
" | grep -E '^\s+status:|^\s+reason:|^With '

# =========================================================
# Applicability: query has no WHERE — no filter predicate
# =========================================================
echo "--- applicability: query has no filter ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_a_minmax ON t_hypo_app (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_app;
" | grep -E '^\s+status:|^\s+reason:|^With '

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_app"
