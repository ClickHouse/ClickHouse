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
    SELECT name, type FROM system.hypothetical_projections WHERE table = 't_hypo_proj_ddl' ORDER BY name;
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
    SELECT name, type FROM system.hypothetical_projections WHERE table = 't_hypo_proj_ddl' AND name = 'p_idx';
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
" 2>&1 | grep -oE 'reason: +Hypothetical projection no longer matches the current table schema' | awk '{$1=$1; print}'

echo "--- a name taken by a real projection is rejected ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL PROJECTION p_real ON t_hypo_proj_ddl (SELECT a, b ORDER BY b);" 2>&1 | grep -m1 -oE 'BAD_ARGUMENTS'

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

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_proj_ddl; DROP TABLE IF EXISTS t_hypo_proj_log; DROP TABLE IF EXISTS t_hypo_proj_drift;"
