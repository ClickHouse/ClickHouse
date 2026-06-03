#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped and not replicated

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# State is session-scoped, so each scenario sets up and asserts within one invocation.

# A fresh session has no hypothetical indexes
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


# A stale entry from the old UUID is removable by name, and re-creating purges it
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


# IF NOT EXISTS / DROP IF EXISTS edge cases (all no-ops; none should error)
echo "--- IF NOT EXISTS / DROP IF EXISTS edge cases ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_dup;
    CREATE TABLE t_hypo_dup (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_a ON t_hypo_dup (a) TYPE minmax GRANULARITY 1;
    SELECT '--- IF NOT EXISTS is silent on duplicate ---';
    CREATE HYPOTHETICAL INDEX IF NOT EXISTS idx_a ON t_hypo_dup (a) TYPE minmax GRANULARITY 1;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_dup';
    SELECT '--- DROP IF EXISTS is silent on missing ---';
    DROP HYPOTHETICAL INDEX IF EXISTS idx_nope ON t_hypo_dup;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_dup';

    DROP TABLE IF EXISTS t_hypo_dup2;
    CREATE TABLE t_hypo_dup2 (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_a ON t_hypo_dup2 (a) TYPE minmax GRANULARITY 1;
    SELECT '--- IF NOT EXISTS is a no-op with an invalid replacement declaration ---';
    CREATE HYPOTHETICAL INDEX IF NOT EXISTS idx_a ON t_hypo_dup2 (missing_col) TYPE minmax GRANULARITY 1;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_dup2';

    DROP TABLE IF EXISTS t_hypo_dup3;
    CREATE TABLE t_hypo_dup3 (a UInt64, b UInt64, INDEX idx_real b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a;
    SELECT '--- IF NOT EXISTS is a no-op when the name matches a real secondary index ---';
    CREATE HYPOTHETICAL INDEX IF NOT EXISTS idx_real ON t_hypo_dup3 (b) TYPE minmax GRANULARITY 1;
    SELECT count() FROM system.hypothetical_indexes WHERE table = 't_hypo_dup3';
" 2>&1 | grep -E '^--- |^[0-9]+$|UNKNOWN_IDENTIFIER|BAD_ARGUMENTS'

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

echo "--- EXPLAIN WHATIF with FINAL reports not_applicable ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_final;
    CREATE TABLE t_hypo_final (a UInt64, b UInt64) ENGINE = ReplacingMergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_final SELECT number, number FROM numbers(100);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_final (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_final FINAL WHERE b = 42;
" | grep -E '^With |^\s+status:|^\s+reason:'

echo "--- CREATE rejects text index (explicit unsupported-type path) ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_text;
    CREATE TABLE t_hypo_text (id UInt32, message String) ENGINE = MergeTree ORDER BY id;
    CREATE HYPOTHETICAL INDEX idx_text ON t_hypo_text (message) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1;
" 2>&1 | grep -m1 -o "of type 'text' are not supported"

echo "--- CREATE rejects vector_similarity index (explicit unsupported-type path) ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_vec;
    CREATE TABLE t_hypo_vec (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
    CREATE HYPOTHETICAL INDEX idx_vec ON t_hypo_vec (v) TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 1;
" 2>&1 | grep -m1 -o "of type 'vector_similarity' are not supported"

echo "--- force_data_skipping_indices: useful hypothetical index is accepted ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_force;
    CREATE TABLE t_hypo_force (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_force SELECT number, number FROM numbers(100);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_force (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_force WHERE b = 42 SETTINGS force_data_skipping_indices = 'idx_b';
" 2>&1 | grep -E '^With |^\s+status:'

echo "--- force_data_skipping_indices: not-useful index throws like a real read ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_force (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_force WHERE a = 1 SETTINGS force_data_skipping_indices = 'idx_b';
" 2>&1 | grep -m1 -o 'INDEX_NOT_USED'

echo "--- force_data_skipping_indices = '' throws like a real read ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_force (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_force WHERE b = 42 SETTINGS force_data_skipping_indices = '';
" 2>&1 | grep -m1 -o 'CANNOT_PARSE_TEXT'

# Inner-SELECT SETTINGS apply on the effective query context, not the outer EXPLAIN context.
echo "--- inner-SELECT SETTINGS apply on the effective query context ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_eff;
    CREATE TABLE t_hypo_eff (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_eff SELECT number, number FROM numbers(100);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_eff (b) TYPE minmax GRANULARITY 1;

    SELECT '--- use_skip_indexes = 0 reports not_applicable ---';
    EXPLAIN WHATIF SELECT * FROM t_hypo_eff WHERE b = 42 SETTINGS use_skip_indexes = 0;

    SELECT '--- use_skip_indexes = 0 makes empty force_data_skipping_indices a no-op ---';
    EXPLAIN WHATIF SELECT * FROM t_hypo_eff WHERE b = 42 SETTINGS use_skip_indexes = 0, force_data_skipping_indices = '';

    SELECT '--- ignore_data_skipping_indices drops the candidate by name ---';
    EXPLAIN WHATIF SELECT * FROM t_hypo_eff WHERE b = 42 SETTINGS ignore_data_skipping_indices = 'idx_b';
" | grep -E '^--- |^With |^\s+status:|^\s+reason:'

# Empty ignore_data_skipping_indices throws on parse, exactly like a real read.
echo "--- empty ignore_data_skipping_indices throws like a real read ---"
$CLICKHOUSE_CLIENT -n -q "
    EXPLAIN WHATIF SELECT * FROM t_hypo_eff WHERE b = 42 SETTINGS ignore_data_skipping_indices = '';
" 2>&1 | grep -m1 -o 'CANNOT_PARSE_TEXT'

# use_skip_indexes_on_data_read defaults on, deferring existing-index pruning to read time;
# the static baseline must hold even when the inner SELECT re-enables it.
echo "--- use_skip_indexes_on_data_read = 1 on the inner SELECT keeps the static baseline ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_skipread;
    CREATE TABLE t_hypo_skipread (a UInt64, b UInt64, INDEX idx_real b TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_skipread SELECT number, number FROM numbers(200);
    CREATE HYPOTHETICAL INDEX idx_h ON t_hypo_skipread (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_skipread WHERE b = 42 SETTINGS use_skip_indexes_on_data_read = 1;
" | grep -E '^  parts:|^  marks:'

# read_overflow_mode = 'break' must not report a partial scan as a complete empirical estimate.
echo "--- read limit (break mode) does not report partial empirical as ok ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_break;
    CREATE TABLE t_hypo_break (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_break SELECT number, number FROM numbers(100);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_break (b) TYPE minmax GRANULARITY 1;
    SET max_rows_to_read = 50, read_overflow_mode = 'break';
    EXPLAIN WHATIF SELECT * FROM t_hypo_break WHERE b = 42;
" 2>&1 | grep -E '^\s+source:|^\s+empirical_status:'

echo "--- EXPLAIN WHATIF with function-expression index ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_func;
    CREATE TABLE t_hypo_func (a UInt64, s String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_func SELECT number, if(number < 100, 'Hit', 'Miss') FROM numbers(100);
    CREATE HYPOTHETICAL INDEX idx_l ON t_hypo_func (lower(s)) TYPE set(100) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_func WHERE lower(s) = 'hit';
" | grep -E '^With |^\s+status:'

# Reserved auto_minmax_index_ prefix is rejected when implicit minmax is enabled.
echo "--- CREATE rejects reserved auto_minmax_index_ name ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_auto;
    CREATE TABLE t_hypo_auto (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS add_minmax_index_for_numeric_columns = 1;
    CREATE HYPOTHETICAL INDEX auto_minmax_index_x ON t_hypo_auto (b) TYPE minmax GRANULARITY 1;
" 2>&1 | grep -m1 -o 'reserved index name'

echo "--- CREATE rejects unknown index type ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_bad;
    CREATE TABLE t_hypo_bad (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX bad ON t_hypo_bad (b) TYPE no_such_type GRANULARITY 1;
" 2>&1 | grep -m1 -oE 'INCORRECT_QUERY|UNKNOWN_FUNCTION|BAD_ARGUMENTS'

echo "--- type_full distinguishes parametrized index types ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_tf;
    CREATE TABLE t_hypo_tf (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX i1 ON t_hypo_tf (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
    CREATE HYPOTHETICAL INDEX i2 ON t_hypo_tf (b) TYPE bloom_filter(0.001) GRANULARITY 1;
    SELECT name, type, type_full FROM system.hypothetical_indexes
    WHERE table = 't_hypo_tf' ORDER BY name FORMAT TSV;
"

# set index on b, predicate on c -> not applicable.
echo "--- applicability: predicate doesn't reference index column ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_app;
    CREATE TABLE t_hypo_app (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_app SELECT number, number, toString(number) FROM numbers(100);

    CREATE HYPOTHETICAL INDEX idx_b_set ON t_hypo_app (b) TYPE set(100) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_app WHERE c = 'foo';
" | grep -E '^\s+status:|^\s+reason:|^With '

# No WHERE -> no filter predicate -> not applicable.
echo "--- applicability: query has no filter ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_a_minmax ON t_hypo_app (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_app;
" | grep -E '^\s+status:|^\s+reason:|^With '


# A standalone conjunct keeps the candidate usable even when it also appears in a mixed OR.
echo "--- applicability: candidate usable via a standalone conjunct beside a mixed OR ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_disj;
    CREATE TABLE t_hypo_disj (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_disj SELECT number, number, number FROM numbers(100);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_disj (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_disj WHERE (b = 1 OR c = 2) AND b = 1;
" | grep -E '^With |^\s+status:'

# Hypothetical indexes are session-local, so WHATIF must stay on the local read.
echo "--- parallel replicas: EXPLAIN WHATIF still runs locally ---"
$CLICKHOUSE_CLIENT --enable_parallel_replicas=1 --parallel_replicas_for_non_replicated_merge_tree=1 --cluster_for_parallel_replicas=parallel_replicas --parallel_replicas_local_plan=1 -n -q "
    DROP TABLE IF EXISTS t_hypo_pr;
    CREATE TABLE t_hypo_pr (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_pr SELECT number, number FROM numbers(100);

    CREATE HYPOTHETICAL INDEX idx_a ON t_hypo_pr (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_pr WHERE a > 50;
" | grep -E '^\s+status:|^\s+source:|^With idx_a'


# Parallel replicas requested via the inner SELECT SETTINGS must still run locally
echo "--- parallel replicas via inner SETTINGS: EXPLAIN WHATIF still runs locally ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_pr2;
    CREATE TABLE t_hypo_pr2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_pr2 SELECT number, number FROM numbers(100);
    CREATE HYPOTHETICAL INDEX idx_a ON t_hypo_pr2 (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_pr2 WHERE a > 50
        SETTINGS enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
                 cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_local_plan = 0;
" | grep -E '^\s+status:|^\s+source:|^With idx_a'

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

# A column-grant user that can CREATE a hypothetical index must also be able to DROP it.
echo "--- column-level SELECT user can create and drop a hypothetical index ---"
user2="u2_04223_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_priv2;
    CREATE TABLE t_hypo_priv2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    DROP USER IF EXISTS ${user2};
    CREATE USER ${user2} NOT IDENTIFIED;
    GRANT SELECT(b) ON ${CLICKHOUSE_DATABASE}.t_hypo_priv2 TO ${user2};
"
$CLICKHOUSE_CLIENT --user "${user2}" -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON ${CLICKHOUSE_DATABASE}.t_hypo_priv2 (b) TYPE minmax GRANULARITY 1;
    DROP HYPOTHETICAL INDEX idx_b ON ${CLICKHOUSE_DATABASE}.t_hypo_priv2;
    SELECT 'create+drop ok';
" 2>&1 | grep -m1 -oE 'create\+drop ok|ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user2}"

# EXPLAIN WHATIF re-checks column-level SELECT at evaluation time, not only at CREATE:
# a grant revoked after CREATE must deny the estimate that would read that column.
echo "--- EXPLAIN WHATIF re-checks column-level SELECT after a grant is revoked ---"
user3="u3_04223_${CLICKHOUSE_DATABASE}"
sess="sess_04223_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_priv3;
    CREATE TABLE t_hypo_priv3 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_priv3 SELECT number, number, number FROM numbers(100);
    DROP USER IF EXISTS ${user3};
    CREATE USER ${user3} NOT IDENTIFIED;
    GRANT SELECT(a, b, c) ON ${CLICKHOUSE_DATABASE}.t_hypo_priv3 TO ${user3};
"
# An HTTP session keeps the session-local store alive across requests; create the index on
# (b, c) while SELECT(c) is granted, revoke it, then evaluate from the same session.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user3}&session_id=${sess}&session_timeout=60" \
    --data-binary "CREATE HYPOTHETICAL INDEX idx_bc ON ${CLICKHOUSE_DATABASE}.t_hypo_priv3 (b, c) TYPE minmax GRANULARITY 1"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(c) ON ${CLICKHOUSE_DATABASE}.t_hypo_priv3 FROM ${user3}"
# A query referencing only b still plans, but evaluating idx_bc reads c, so it is denied.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user3}&session_id=${sess}&session_timeout=60" \
    --data-binary "EXPLAIN WHATIF SELECT b FROM ${CLICKHOUSE_DATABASE}.t_hypo_priv3 WHERE b = 42" 2>&1 | grep -m1 -o 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user3}"

# Empirical EXPLAIN WHATIF reads table data, so it must consume and respect the read quota.
echo "--- empirical EXPLAIN WHATIF consumes and respects READ_ROWS quota ---"
userq="uq_04223_${CLICKHOUSE_DATABASE}"
quotaq="q_04223_${CLICKHOUSE_DATABASE}"
sessq="sessq_04223_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_quota;
    CREATE TABLE t_hypo_quota (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_quota SELECT number, number FROM numbers(200);
    DROP USER IF EXISTS ${userq};
    CREATE USER ${userq} NOT IDENTIFIED;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_hypo_quota TO ${userq};
    DROP QUOTA IF EXISTS ${quotaq};
    CREATE QUOTA ${quotaq} FOR INTERVAL 1 hour MAX read_rows = 100 TO ${userq};
"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${userq}&session_id=${sessq}&session_timeout=60" \
    --data-binary "CREATE HYPOTHETICAL INDEX idx_b ON ${CLICKHOUSE_DATABASE}.t_hypo_quota (b) TYPE minmax GRANULARITY 1"
# The scan reads 200 rows, over the 100-row quota, so the estimate is denied.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${userq}&session_id=${sessq}&session_timeout=60" \
    --data-binary "EXPLAIN WHATIF SELECT * FROM ${CLICKHOUSE_DATABASE}.t_hypo_quota WHERE b = 42" 2>&1 | grep -m1 -o 'QUOTA_EXCEEDED'
$CLICKHOUSE_CLIENT -q "DROP QUOTA IF EXISTS ${quotaq}"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${userq}"

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_lc;
    DROP TABLE IF EXISTS t_hypo_stale;
    DROP TABLE IF EXISTS t_hypo_dup;
    DROP TABLE IF EXISTS t_hypo_dup2;
    DROP TABLE IF EXISTS t_hypo_dup3;
    DROP TABLE IF EXISTS t_hypo_orphan;
    DROP TABLE IF EXISTS t_hypo_susp;
    DROP TABLE IF EXISTS t_hypo_final;
    DROP TABLE IF EXISTS t_hypo_text;
    DROP TABLE IF EXISTS t_hypo_vec;
    DROP TABLE IF EXISTS t_hypo_force;
    DROP TABLE IF EXISTS t_hypo_eff;
    DROP TABLE IF EXISTS t_hypo_skipread;
    DROP TABLE IF EXISTS t_hypo_break;
    DROP TABLE IF EXISTS t_hypo_func;
    DROP TABLE IF EXISTS t_hypo_auto;
    DROP TABLE IF EXISTS t_hypo_bad;
    DROP TABLE IF EXISTS t_hypo_tf;
    DROP TABLE IF EXISTS t_hypo_app;
    DROP TABLE IF EXISTS t_hypo_disj;
    DROP TABLE IF EXISTS t_hypo_pr;
    DROP TABLE IF EXISTS t_hypo_pr2;
    DROP TABLE IF EXISTS t_hypo_priv;
    DROP TABLE IF EXISTS t_hypo_priv2;
    DROP TABLE IF EXISTS t_hypo_priv3;
    DROP TABLE IF EXISTS t_hypo_quota;
"
