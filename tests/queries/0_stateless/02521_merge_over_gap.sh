#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib
set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_with_gap;"

$CLICKHOUSE_CLIENT --query "CREATE TABLE table_with_gap (v UInt8) ENGINE = MergeTree() ORDER BY tuple() settings old_parts_lifetime = 10000;"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES table_with_gap;"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO table_with_gap VALUES (1);
    INSERT INTO table_with_gap VALUES (2);
    INSERT INTO table_with_gap VALUES (3);
    INSERT INTO table_with_gap VALUES (4);
"

$CLICKHOUSE_CLIENT --query "SELECT 'initial parts';"
$CLICKHOUSE_CLIENT --query "SELECT name, rows, active FROM system.parts WHERE table = 'table_with_gap' AND database = currentDatabase();"

$CLICKHOUSE_CLIENT --query "ALTER TABLE table_with_gap DROP PART 'all_3_3_0';"

$CLICKHOUSE_CLIENT --query "SELECT 'parts with gap';"
$CLICKHOUSE_CLIENT --query "SELECT name, rows, active FROM system.parts WHERE table = 'table_with_gap' AND database = currentDatabase();"

$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES table_with_gap;"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE table_with_gap FINAL SETTINGS optimize_throw_if_noop=1;" 2>&1 |\
  grep "There is an outdated part in a gap between two active parts" |\
  grep -o "CANNOT_ASSIGN_OPTIMIZE" | uniq || echo "NO ERROR"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE table_with_gap;"

$CLICKHOUSE_CLIENT --query "SELECT 'parts after optimize';"
$CLICKHOUSE_CLIENT --query "SELECT name, rows, active FROM system.parts WHERE table = 'table_with_gap' AND database = currentDatabase();"

$CLICKHOUSE_CLIENT --query "DETACH TABLE table_with_gap;"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE table_with_gap;"

$CLICKHOUSE_CLIENT --query "SELECT 'parts after detach/attach';"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT LOADING PARTS table_with_gap;"
$CLICKHOUSE_CLIENT --query "SELECT name, rows, active FROM system.parts WHERE table = 'table_with_gap' AND database = currentDatabase();"
