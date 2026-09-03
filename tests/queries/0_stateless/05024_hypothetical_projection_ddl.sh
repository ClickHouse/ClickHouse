#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_proj_ddl;
    CREATE TABLE t_hypo_proj_ddl (a UInt64, b UInt64, PROJECTION p_real (SELECT a, b ORDER BY b))
    ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_proj_ddl SELECT number, number % 100 FROM numbers(1000);
"

echo "--- create, introspect, drop ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_norm ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_agg ON t_hypo_proj_ddl (SELECT b, sum(a) GROUP BY b);
    SELECT name, type, sorting_key FROM system.hypothetical_projections WHERE table = 't_hypo_proj_ddl' ORDER BY name;
    DROP HYPOTHETICAL PROJECTION p_agg ON t_hypo_proj_ddl;
    SELECT 'after drop:', count() FROM system.hypothetical_projections WHERE table = 't_hypo_proj_ddl';
"

echo "--- DROP ALL is per kind, indexes are untouched ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_ddl (b) TYPE minmax GRANULARITY 1;
    CREATE HYPOTHETICAL PROJECTION p_norm ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);
    DROP ALL HYPOTHETICAL PROJECTIONS;
    SELECT 'projections:', count() FROM system.hypothetical_projections WHERE table = 't_hypo_proj_ddl';
    SELECT 'indexes:', count() FROM system.hypothetical_indexes WHERE table = 't_hypo_proj_ddl';
"

echo "--- and the mirror direction: dropping all indexes keeps projections ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_ddl (b) TYPE minmax GRANULARITY 1;
    CREATE HYPOTHETICAL PROJECTION p_norm ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);
    DROP ALL HYPOTHETICAL INDEXES;
    SELECT 'projections:', count() FROM system.hypothetical_projections WHERE table = 't_hypo_proj_ddl';
    SELECT 'indexes:', count() FROM system.hypothetical_indexes WHERE table = 't_hypo_proj_ddl';
"

echo "--- an entry whose table was dropped is hidden ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_proj_gone;
    CREATE TABLE t_hypo_proj_gone (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL PROJECTION p_gone ON t_hypo_proj_gone (SELECT a ORDER BY a);
    DROP TABLE t_hypo_proj_gone;
    SELECT count() FROM system.hypothetical_projections WHERE name = 'p_gone';
"

echo "--- IF NOT EXISTS / IF EXISTS ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_norm ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION IF NOT EXISTS p_norm ON t_hypo_proj_ddl (SELECT a ORDER BY a);
    DROP HYPOTHETICAL PROJECTION IF EXISTS no_such ON t_hypo_proj_ddl;
    SELECT 'still one:', count() FROM system.hypothetical_projections WHERE table = 't_hypo_proj_ddl';
"

# the INDEX ... TYPE ... form of a projection declaration, same as real ADD PROJECTION accepts
echo "--- the INDEX form of a projection declaration is accepted ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_idx ON t_hypo_proj_ddl INDEX b TYPE basic;
    SELECT name, type, query = '' AS query_is_empty, definition
    FROM system.hypothetical_projections WHERE table = 't_hypo_proj_ddl' AND name = 'p_idx';
"

echo "--- WITH SETTINGS survives into the system table ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_set ON t_hypo_proj_ddl (SELECT a, b ORDER BY b) WITH SETTINGS (index_granularity = 64);
    SELECT name, settings FROM system.hypothetical_projections WHERE table = 't_hypo_proj_ddl' AND name = 'p_set';
"

echo "--- a projection invalidated by ALTER reports schema drift ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_proj_drift;
    CREATE TABLE t_hypo_proj_drift (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_proj_drift SELECT number, number % 100, number FROM numbers(100);
    CREATE HYPOTHETICAL PROJECTION p_c ON t_hypo_proj_drift (SELECT a, c ORDER BY c);
    ALTER TABLE t_hypo_proj_drift DROP COLUMN c SETTINGS mutations_sync = 2;
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_drift WHERE b = 1;
" 2>&1 | grep -oE 'reason: +Hypothetical projection can no longer be added to this table' | awk '{$1=$1; print}'

# the same MergeTree projection gates the real ADD PROJECTION enforces
echo "--- commit_order needs the same gates as a real projection ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL PROJECTION p_co ON t_hypo_proj_ddl INDEX b TYPE commit_order;" 2>&1 | grep -m1 -oE 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_proj_co;
    CREATE TABLE t_hypo_proj_co (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS allow_commit_order_projection = 1, enable_block_number_column = 1, enable_block_offset_column = 1;
    CREATE HYPOTHETICAL PROJECTION p_co ON t_hypo_proj_co INDEX b TYPE commit_order;
    SELECT 'accepted with gates on:', count() FROM system.hypothetical_projections WHERE table = 't_hypo_proj_co';
"

echo "--- disabling those gates later makes it not_applicable ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_co ON t_hypo_proj_co INDEX b TYPE commit_order;
    ALTER TABLE t_hypo_proj_co MODIFY SETTING allow_commit_order_projection = 0;
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_co WHERE b = 1;
" 2>&1 | grep -oE 'reason: +Hypothetical projection can no longer be added to this table' | awk '{$1=$1; print}'

# allow_non_metadata_alters gates data-modifying ALTERs; ADD PROJECTION is metadata-only, so a
# valid definition must still be accepted with it off
echo "--- a runtime ALTER guard does not reject a valid definition ---"
$CLICKHOUSE_CLIENT -q "
    SET allow_non_metadata_alters = 0;
    CREATE HYPOTHETICAL PROJECTION p_nma ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);
    SELECT 'created:', count() FROM system.hypothetical_projections WHERE name = 'p_nma';
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_ddl WHERE b = 42 SETTINGS optimize_use_projections = 0;
" 2>&1 | grep -oE "created:.*|reason: +EXPLAIN WHATIF does not estimate.*" | awk '{$1=$1; print}'

echo "--- a name taken by a real projection is rejected ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL PROJECTION p_real ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);" 2>&1 | grep -m1 -oE 'ILLEGAL_PROJECTION'

echo "--- duplicate without IF NOT EXISTS is rejected ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_norm ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_norm ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);
" 2>&1 | grep -m1 -oE 'BAD_ARGUMENTS'

echo "--- non-MergeTree is rejected ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_proj_log;
    CREATE TABLE t_hypo_proj_log (a UInt64) ENGINE = Log;
    CREATE HYPOTHETICAL PROJECTION p ON t_hypo_proj_log (SELECT a ORDER BY a);
" 2>&1 | grep -m1 -oE 'NOT_IMPLEMENTED'

echo "--- EXPLAIN WHATIF lists the projection as not estimated yet ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_norm ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_ddl WHERE b = 42 SETTINGS optimize_use_projections = 0;
" 2>&1 | grep -oE 'With p_norm \(projection \(normal\), hypothetical\):|status: +not_applicable|reason: +EXPLAIN WHATIF does not estimate.*' | awk '{$1=$1; print}'

echo "--- with nothing defined the report says so ---"
$CLICKHOUSE_CLIENT -q "EXPLAIN WHATIF SELECT a FROM t_hypo_proj_ddl WHERE b = 42 SETTINGS optimize_use_projections = 0;" 2>&1 | grep -oE 'No hypothetical indexes or projections defined.*' | head -1

# Without ALTER ADD PROJECTION the statement must not resolve the definition, otherwise a caller
# could tell an existing column from a missing one by the error they get back
echo "--- creating needs the same privilege as a real ADD PROJECTION ---"
user="u_05024_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}; CREATE USER ${user} NOT IDENTIFIED;"
$CLICKHOUSE_CLIENT --user "${user}" -q "CREATE HYPOTHETICAL PROJECTION p_priv ON ${CLICKHOUSE_DATABASE}.t_hypo_proj_ddl (SELECT a, b ORDER BY b);" 2>&1 | grep -m1 -o 'ACCESS_DENIED'
# a missing column must give the same answer, not UNKNOWN_IDENTIFIER
$CLICKHOUSE_CLIENT --user "${user}" -q "CREATE HYPOTHETICAL PROJECTION p_priv ON ${CLICKHOUSE_DATABASE}.t_hypo_proj_ddl (SELECT nosuchcol ORDER BY nosuchcol);" 2>&1 | grep -m1 -oE 'ACCESS_DENIED|UNKNOWN_IDENTIFIER'
echo "--- dropping needs it too, so the drop cannot probe the table either ---"
$CLICKHOUSE_CLIENT --user "${user}" -q "DROP HYPOTHETICAL PROJECTION IF EXISTS p_priv ON ${CLICKHOUSE_DATABASE}.t_hypo_proj_ddl;" 2>&1 | grep -m1 -oE 'ACCESS_DENIED' || echo "no error"
# a table that does not exist must give the same answer, not UNKNOWN_TABLE
$CLICKHOUSE_CLIENT --user "${user}" -q "DROP HYPOTHETICAL PROJECTION IF EXISTS p_priv ON ${CLICKHOUSE_DATABASE}.t_no_such_table;" 2>&1 | grep -m1 -oE 'ACCESS_DENIED|UNKNOWN_TABLE' || echo "no error"
$CLICKHOUSE_CLIENT -q "GRANT ALTER ADD PROJECTION ON ${CLICKHOUSE_DATABASE}.t_hypo_proj_ddl TO ${user};"
# that privilege alone is enough; reading the columns is not required until estimation exists
$CLICKHOUSE_CLIENT --user "${user}" -q "CREATE HYPOTHETICAL PROJECTION p_priv ON ${CLICKHOUSE_DATABASE}.t_hypo_proj_ddl (SELECT a, b ORDER BY b); SELECT 'granted user can create';" 2>&1 | grep -m1 -oE 'granted user can create|ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "${user}" -q "DROP HYPOTHETICAL PROJECTION IF EXISTS p_priv ON ${CLICKHOUSE_DATABASE}.t_hypo_proj_ddl; SELECT 'granted user can drop';" 2>&1 | grep -m1 -oE 'granted user can drop|ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user};"


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_proj_ddl; DROP TABLE IF EXISTS t_hypo_proj_log; DROP TABLE IF EXISTS t_hypo_proj_drift; DROP TABLE IF EXISTS t_hypo_proj_co;"
