#!/usr/bin/env bash
# a policy can be there before the table, so creating a merging table under one needs the same opt-in

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_table ON $CLICKHOUSE_DATABASE.t_summing USING secret < 100 TO ALL"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_summing (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant" 2>&1 | grep -o -m1 "SummingMergeTree engine"

# the same policy over a table which keeps its rows apart is fine
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_summing (tenant String, secret UInt64) ENGINE = MergeTree ORDER BY tenant"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_summing"

$CLICKHOUSE_CLIENT --allow_suspicious_row_policies_with_blending_engines 1 -q "CREATE TABLE t_summing (tenant String, secret UInt64) ENGINE = SummingMergeTree ORDER BY tenant"
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_summing'"

# ATTACH must not be blocked, otherwise a server restart cannot load the table
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_summing"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_summing"
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_summing'"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_table ON $CLICKHOUSE_DATABASE.t_summing"

# a policy for the whole database counts as well
$CLICKHOUSE_CLIENT --allow_suspicious_row_policies_with_blending_engines 1 -q "CREATE ROW POLICY p_db ON $CLICKHOUSE_DATABASE.* USING secret < 100 TO ALL"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_coalescing (tenant String, secret Nullable(String)) ENGINE = CoalescingMergeTree ORDER BY tenant" 2>&1 | grep -o -m1 "CoalescingMergeTree engine"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_db ON $CLICKHOUSE_DATABASE.*"
