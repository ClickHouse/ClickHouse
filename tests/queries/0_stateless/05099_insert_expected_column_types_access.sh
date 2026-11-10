#!/usr/bin/env bash
# insert_expected_column_types names only columns the user may see: a hidden column is neither confirmed nor typed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_05099_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_expected_access;
    DROP USER IF EXISTS $user;
    CREATE TABLE t_expected_access (x UInt8, secret UUID) ENGINE = Memory;
    CREATE USER $user;
    GRANT INSERT(x) ON ${CLICKHOUSE_DATABASE}.t_expected_access TO $user;
"

run() { $CLICKHOUSE_CLIENT --user "$user" --async_insert 0 -q "$1" 2>&1 | grep -oE 'ACCESS_DENIED|INCOMPATIBLE_SCHEMA' | head -1 | grep . || echo OK; }

echo "hidden column: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'secret': 'String'} VALUES (1)")"
echo "missing column: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'nope': 'String'} VALUES (1)")"
echo "granted column, right type: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'x': 'UInt8'} VALUES (2)")"
echo "granted column, wrong type: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'x': 'String'} VALUES (3)")"

$CLICKHOUSE_CLIENT -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_expected_access TO $user"
echo "table grant, wrong type: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'secret': 'String'} VALUES (4)")"
echo "table grant, right type: $(run "INSERT INTO t_expected_access (x) SETTINGS insert_expected_column_types = {'secret': 'UUID'} VALUES (5)")"

$CLICKHOUSE_CLIENT -q "SELECT groupArray(x) FROM (SELECT x FROM t_expected_access ORDER BY x)"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_expected_access; DROP USER $user"
