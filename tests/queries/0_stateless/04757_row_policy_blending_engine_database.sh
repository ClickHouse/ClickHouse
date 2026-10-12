#!/usr/bin/env bash
# a policy for the whole database is used for the merging tables in it, so it needs the same opt-in

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_summing (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant"

$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_db ON $CLICKHOUSE_DATABASE.* USING secret < 100 TO ALL" 2>&1 | grep -o -m1 "SummingMergeTree engine"

$CLICKHOUSE_CLIENT --allow_suspicious_row_policies_with_blending_engines 1 -q "CREATE ROW POLICY p_db ON $CLICKHOUSE_DATABASE.* USING secret < 100 TO ALL"
$CLICKHOUSE_CLIENT -q "SELECT short_name FROM system.row_policies WHERE database = '$CLICKHOUSE_DATABASE'"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_db ON $CLICKHOUSE_DATABASE.*"

# a database without such tables is not affected
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_plain"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_plain.t (tenant String, secret UInt64) ENGINE = MergeTree ORDER BY tenant"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_db ON ${CLICKHOUSE_DATABASE}_plain.* USING secret < 100 TO ALL"
$CLICKHOUSE_CLIENT -q "SELECT short_name FROM system.row_policies WHERE database = '${CLICKHOUSE_DATABASE}_plain'"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_db ON ${CLICKHOUSE_DATABASE}_plain.*"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_plain"
