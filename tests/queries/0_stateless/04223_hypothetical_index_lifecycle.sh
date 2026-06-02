#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped and not replicated

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# State is session-scoped, so each scenario sets up and asserts within one invocation.

# A fresh session has no hypothetical indexes.
echo "--- empty system table ---"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.hypothetical_indexes"

# Store keys on UUID: after DROP/CREATE the old index must not apply to the new table.
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

# A stale entry from the old UUID is removable by name, and re-creating purges it.
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

# IF NOT EXISTS is silent on duplicate; DROP IF EXISTS is silent on missing.
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

# IF NOT EXISTS short-circuits before validating an otherwise-invalid replacement.
echo "--- IF NOT EXISTS is a no-op even with an invalid replacement declaration ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_dup2;
    CREATE TABLE t_hypo_dup2 (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_a ON t_hypo_dup2 (a) TYPE minmax GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX IF NOT EXISTS idx_a ON t_hypo_dup2 (missing_col) TYPE minmax GRANULARITY 1;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_dup2';
" 2>&1 | grep -E '^[0-9]+$|UNKNOWN_IDENTIFIER'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_dup2"

# IF NOT EXISTS is also a no-op when the name matches an existing real secondary index.
echo "--- IF NOT EXISTS is a no-op when the name matches a real secondary index ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_dup3;
    CREATE TABLE t_hypo_dup3 (a UInt64, b UInt64, INDEX idx_real b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX IF NOT EXISTS idx_real ON t_hypo_dup3 (b) TYPE minmax GRANULARITY 1;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_dup3';
" 2>&1 | grep -E '^[0-9]+$|BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_dup3"

echo "--- DROP TABLE hides the entry from system.hypothetical_indexes ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_orphan;
    CREATE TABLE t_hypo_orphan (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_orphan (b) TYPE minmax GRANULARITY 1;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_orphan';
    DROP TABLE t_hypo_orphan;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_orphan';
" | grep -E '^[0-9]+$'

echo "--- CREATE respects allow_suspicious_indices = 0 ---"
$CLICKHOUSE_CLIENT --allow_suspicious_indices 0 -n -q "
    DROP TABLE IF EXISTS t_hypo_susp;
    CREATE TABLE t_hypo_susp (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_dup ON t_hypo_susp (a, a) TYPE minmax GRANULARITY 1;
" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_susp"

echo "--- EXPLAIN WHATIF with FINAL reports not_applicable ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_final;
    CREATE TABLE t_hypo_final (a UInt64, b UInt64) ENGINE = ReplacingMergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_final SELECT number, number FROM numbers(1000);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_final (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_final FINAL WHERE b = 42;
" | grep -E '^With |^\s+status:|^\s+reason:'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_final"

echo "--- CREATE rejects text index (explicit unsupported-type path) ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_text;
    CREATE TABLE t_hypo_text (id UInt32, message String) ENGINE = MergeTree ORDER BY id;
    CREATE HYPOTHETICAL INDEX idx_text ON t_hypo_text (message) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1;
" 2>&1 | grep -m1 -o "of type 'text' are not supported"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_text"

echo "--- CREATE rejects vector_similarity index (explicit unsupported-type path) ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_vec;
    CREATE TABLE t_hypo_vec (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
    CREATE HYPOTHETICAL INDEX idx_vec ON t_hypo_vec (v) TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 1;
" 2>&1 | grep -m1 -o "of type 'vector_similarity' are not supported"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_vec"

echo "--- force_data_skipping_indices: useful hypothetical index is accepted ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_force;
    CREATE TABLE t_hypo_force (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_force SELECT number, number FROM numbers(1000);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_force (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_force WHERE b = 42 SETTINGS force_data_skipping_indices = 'idx_b';
" 2>&1 | grep -E '^With |^\s+status:'

echo "--- force_data_skipping_indices: not-useful index throws like a real read ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_force (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_force WHERE a = 1 SETTINGS force_data_skipping_indices = 'idx_b';
" 2>&1 | grep -m1 -o 'INDEX_NOT_USED'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_force"

# read_overflow_mode = 'break' must not report a partial scan as a complete empirical estimate.
echo "--- force_data_skipping_indices = '' throws like a real read ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_force2;
    CREATE TABLE t_hypo_force2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_force2 SELECT number, number FROM numbers(100);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_force2 (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_force2 WHERE b = 42 SETTINGS force_data_skipping_indices = '';
" 2>&1 | grep -m1 -o 'CANNOT_PARSE_TEXT'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_force2"

echo "--- read limit (break mode) does not report partial empirical as ok ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_break;
    CREATE TABLE t_hypo_break (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_break SELECT number, number FROM numbers(1000);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_break (b) TYPE minmax GRANULARITY 1;
    SET max_rows_to_read = 50, read_overflow_mode = 'break';
    EXPLAIN WHATIF SELECT * FROM t_hypo_break WHERE b = 42;
" 2>&1 | grep -E '^\s+source:|^\s+empirical_status:'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_break"

echo "--- EXPLAIN WHATIF with function-expression index ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_func;
    CREATE TABLE t_hypo_func (a UInt64, s String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_func SELECT number, if(number < 100, 'Hit', 'Miss') FROM numbers(1000);
    CREATE HYPOTHETICAL INDEX idx_l ON t_hypo_func (lower(s)) TYPE set(100) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_func WHERE lower(s) = 'hit';
" | grep -E '^With |^\s+status:'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_func"

# Reserved auto_minmax_index_ prefix is rejected when implicit minmax is enabled.
echo "--- CREATE rejects reserved auto_minmax_index_ name ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_auto;
    CREATE TABLE t_hypo_auto (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS add_minmax_index_for_numeric_columns = 1;
    CREATE HYPOTHETICAL INDEX auto_minmax_index_x ON t_hypo_auto (b) TYPE minmax GRANULARITY 1;
" 2>&1 | grep -m1 -o 'reserved index name'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_auto"

echo "--- CREATE rejects unknown index type ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_bad;
    CREATE TABLE t_hypo_bad (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX bad ON t_hypo_bad (b) TYPE no_such_type GRANULARITY 1;
" 2>&1 | grep -m1 -oE 'INCORRECT_QUERY|UNKNOWN_FUNCTION|BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_bad"

echo "--- type_full distinguishes parametrized index types ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_tf;
    CREATE TABLE t_hypo_tf (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX i1 ON t_hypo_tf (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX i2 ON t_hypo_tf (b) TYPE bloom_filter(0.001) GRANULARITY 1;
    SELECT name, type, type_full FROM system.hypothetical_indexes
    WHERE table = 't_hypo_tf' ORDER BY name FORMAT TSV;
"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_tf"

# set index on b, predicate on c -> not applicable.
echo "--- applicability: predicate doesn't reference index column ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_app;
    CREATE TABLE t_hypo_app (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_app SELECT number, number, toString(number) FROM numbers(1000);

    CREATE HYPOTHETICAL INDEX idx_b_set ON t_hypo_app (b) TYPE set(100) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_app WHERE c = 'foo';
" | grep -E '^\s+status:|^\s+reason:|^With '

# No WHERE -> no filter predicate -> not applicable.
echo "--- applicability: query has no filter ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_a_minmax ON t_hypo_app (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_app;
" | grep -E '^\s+status:|^\s+reason:|^With '

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_app"

# Hypothetical indexes are session-local, so WHATIF must stay on the local read.
echo "--- parallel replicas: EXPLAIN WHATIF still runs locally ---"
$CLICKHOUSE_CLIENT --enable_parallel_replicas=1 --parallel_replicas_for_non_replicated_merge_tree=1 --cluster_for_parallel_replicas=parallel_replicas --parallel_replicas_local_plan=1 -n -q "
    DROP TABLE IF EXISTS t_hypo_pr;
    CREATE TABLE t_hypo_pr (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_pr SELECT number, number FROM numbers(1000);

    CREATE HYPOTHETICAL INDEX idx_a ON t_hypo_pr (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_pr WHERE a > 500;
" | grep -E '^\s+status:|^\s+source:|^With idx_a'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_pr"

# Column-level SELECT is required: a user without access to the index column is denied.
echo "--- CREATE requires column-level SELECT on the index column ---"
user="u_04223_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_priv;
    CREATE TABLE t_hypo_priv (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_hypo_priv TO ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "
    CREATE HYPOTHETICAL INDEX idx_b ON ${CLICKHOUSE_DATABASE}.t_hypo_priv (b) TYPE minmax GRANULARITY 1;
" 2>&1 | grep -m1 -o 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_priv"
