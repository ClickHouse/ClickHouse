#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# All hypothetical-index state is session-scoped, so each scenario uses
# a single $CLICKHOUSE_CLIENT invocation that sets up and asserts what it needs.

# =========================================================
# Empty system.hypothetical_indexes — a fresh session has none.
# =========================================================
echo "--- empty system table ---"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.hypothetical_indexes"

# =========================================================
# Drop and recreate the table — the old hypothetical index must NOT
# apply to the new table (the store keys on UUID).
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
# the index on the new table must purge it so it doesn't pile up.
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

# =========================================================
# IF NOT EXISTS is silent on duplicate; second CREATE with no IF errors.
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
# Applicability: set index on column b, predicate on column c → not applicable.
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
# Applicability: query has no WHERE — no filter predicate.
# =========================================================
echo "--- applicability: query has no filter ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_a_minmax ON t_hypo_app (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_app;
" | grep -E '^\s+status:|^\s+reason:|^With '

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_app"

# =========================================================
# EXPLAIN WHATIF must work even when the session enables parallel
# replicas — hypothetical indexes are session-local to the initiator,
# so the WHATIF plan must stay on the local MergeTree read.
# =========================================================
echo "--- parallel replicas: EXPLAIN WHATIF still runs locally ---"
$CLICKHOUSE_CLIENT --enable_parallel_replicas=1 --parallel_replicas_for_non_replicated_merge_tree=1 --cluster_for_parallel_replicas=parallel_replicas --parallel_replicas_local_plan=1 -n -q "
    DROP TABLE IF EXISTS t_hypo_pr;
    CREATE TABLE t_hypo_pr (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_pr SELECT number, number FROM numbers(1000);

    CREATE HYPOTHETICAL INDEX idx_a ON t_hypo_pr (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_pr WHERE a > 500;
" | grep -E '^\s+status:|^\s+source:|^With idx_a'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_pr"
