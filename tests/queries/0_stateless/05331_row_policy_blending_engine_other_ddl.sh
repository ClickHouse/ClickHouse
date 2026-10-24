#!/usr/bin/env bash
# Tags: no-ordinary-database, no-shared-merge-tree
# no-shared-merge-tree: the output pins the engine names
# every DDL which puts a merging table under a row policy needs the opt-in, not only CREATE

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function error()
{
    grep -o -m1 -E "(has|reads a table with) the SummingMergeTree engine"
}

$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t_summing (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant;
CREATE TABLE t_plain (tenant String, secret UInt64) ENGINE = MergeTree ORDER BY tenant;
CREATE TABLE t_alias ENGINE = Alias('t_summing');
"

echo "policy on a table which reads a merging table"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_alias ON t_alias USING secret < 100 TO ALL" 2>&1 | error

echo "table which reads a merging table, created under a policy"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_alias_later ON t_alias_later USING secret < 100 TO ALL"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_alias_later ENGINE = Alias('t_summing')" 2>&1 | error

echo "IF NOT EXISTS and IF EXISTS which change nothing"
$CLICKHOUSE_CLIENT --allow_suspicious_row_policies_with_blending_engines 1 -q "CREATE ROW POLICY p_keep ON t_summing USING secret < 100 TO ALL"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY IF NOT EXISTS p_keep ON t_summing USING secret < 200 TO ALL"
$CLICKHOUSE_CLIENT -q "ALTER ROW POLICY IF EXISTS p_missing ON t_summing USING secret < 200"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY IF NOT EXISTS p_new ON t_summing USING secret < 100 TO ALL" 2>&1 | error
$CLICKHOUSE_CLIENT -q "ALTER ROW POLICY IF EXISTS p_keep ON t_summing USING secret < 200" 2>&1 | error
$CLICKHOUSE_CLIENT -q "SELECT short_name, select_filter FROM system.row_policies WHERE database = currentDatabase() AND table = 't_summing'"

echo "ATTACH with a full definition"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_attach ON t_attach USING secret < 100 TO ALL"
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_attach UUID '$UUID' (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant" 2>&1 | error

echo "CREATE OR REPLACE and REPLACE"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_plain ON t_plain USING secret < 100 TO ALL"
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE TABLE t_plain (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant" 2>&1 | error
$CLICKHOUSE_CLIENT -q "REPLACE TABLE t_plain (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant" 2>&1 | error
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE TABLE t_replaced (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant" 2>&1 | error
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name NOT LIKE 't_alias%' ORDER BY name"

echo "RENAME and EXCHANGE"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_alias"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_renamed ON t_renamed USING secret < 100 TO ALL"
$CLICKHOUSE_CLIENT -q "RENAME TABLE t_summing TO t_renamed" 2>&1 | error
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES t_summing AND t_plain" 2>&1 | error
# the policy on the name already covered a merging table
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_summing_2 (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant"
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES t_summing AND t_summing_2"
$CLICKHOUSE_CLIENT --allow_suspicious_row_policies_with_blending_engines 1 -q "RENAME TABLE t_summing TO t_renamed"
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name"

echo "lazy table which is not loaded yet"
LAZY_DB="${CLICKHOUSE_DATABASE}_lazy"
cleanup() { $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${LAZY_DB} SYNC"; }
trap cleanup EXIT
$CLICKHOUSE_CLIENT -m -q "
CREATE DATABASE ${LAZY_DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE ${LAZY_DB}.t_lazy (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant;
"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${LAZY_DB} SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${LAZY_DB}"
# with no policy for it, an Alias does not load its target
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_alias_lazy ENGINE = Alias('${LAZY_DB}', 't_lazy')"
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database = '${LAZY_DB}'"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_lazy ON ${LAZY_DB}.t_lazy USING secret < 100 TO ALL" 2>&1 | error
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database = '${LAZY_DB}'"

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY IF EXISTS p_lazy ON ${LAZY_DB}.t_lazy;
DROP ROW POLICY IF EXISTS p_alias ON t_alias;
DROP ROW POLICY IF EXISTS p_alias_later ON t_alias_later;
DROP ROW POLICY IF EXISTS p_keep ON t_summing;
DROP ROW POLICY IF EXISTS p_new ON t_summing;
DROP ROW POLICY IF EXISTS p_attach ON t_attach;
DROP ROW POLICY IF EXISTS p_plain ON t_plain;
DROP ROW POLICY IF EXISTS p_renamed ON t_renamed;
"
