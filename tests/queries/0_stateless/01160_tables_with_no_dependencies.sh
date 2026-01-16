#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists t0"
$CLICKHOUSE_CLIENT -q "drop table if exists t1"

# These queries don't imply any loading dependencies but still must work correctly.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t0 (c0 Int STATISTICS(MinMax((1 AS x) IN (*)))) ENGINE = Memory" --allow_experimental_statistics 1
$CLICKHOUSE_CLIENT -q "CREATE TABLE t1 (c0 Int) ENGINE = Distributed('default', 'd0', 't0', COLUMNS(''))" 2>&1 | grep -Fa "Unknown column" >/dev/null && echo "OK"

$CLICKHOUSE_CLIENT -q "SELECT table, arraySort(dependencies_table), arraySort(loading_dependencies_table), arraySort(loading_dependent_table) FROM system.tables WHERE database=currentDatabase() ORDER BY table"

$CLICKHOUSE_CLIENT -q "DROP TABLE t0" # Only table `t0` was actually created, table `t1` wasn't.
