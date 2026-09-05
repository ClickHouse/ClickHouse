#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

user_name="user_04514_a_${CLICKHOUSE_DATABASE}"
table1="table1_04514_${CLICKHOUSE_DATABASE}"
table2="table2_04514_${CLICKHOUSE_DATABASE}"
merge_spec="merge('${CLICKHOUSE_DATABASE}', 'table[12]_04514_${CLICKHOUSE_DATABASE}')"

query_as_user()
{
    local query="$1"
    $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="${user_name}" --query="$query"
}

error_as_user()
{
    local query="$1"
    $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="${user_name}" --query="$query" 2>&1 || true
}

cleanup()
{
    $CLICKHOUSE_CLIENT --query "
        DROP USER IF EXISTS ${user_name};
        DROP TABLE IF EXISTS ${table1};
        DROP TABLE IF EXISTS ${table2};
    "
}

trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${table1}(x UInt32) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE ${table2}(x UInt32) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO ${table1} VALUES (1);
    INSERT INTO ${table2} VALUES (2);
"

echo "merge"
[ "$($CLICKHOUSE_CLIENT --query "SELECT * FROM ${merge_spec} ORDER BY x")" = $'1\n2' ]
$CLICKHOUSE_CLIENT --query "CREATE USER ${user_name}"
grep -q "no tables satisfied provided regexp" <<<"$(error_as_user "SELECT * FROM ${merge_spec} ORDER BY x")"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${table1} TO ${user_name}"
[ "$(query_as_user "SELECT * FROM ${merge_spec} ORDER BY x")" = "1" ]
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user_name}"
[ "$(query_as_user "SELECT * FROM ${merge_spec} ORDER BY x")" = $'1\n2' ]
$CLICKHOUSE_CLIENT --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.${table1} FROM ${user_name}"
[ "$(query_as_user "SELECT * FROM ${merge_spec} ORDER BY x")" = "2" ]
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON ${CLICKHOUSE_DATABASE}.* FROM ${user_name}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${table1} TO ${user_name}"
$CLICKHOUSE_CLIENT --query "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.${table2} TO ${user_name}"
grep -q "it's necessary to have the grant SELECT(x) ON ${CLICKHOUSE_DATABASE}.${table2}" <<<"$(error_as_user "SELECT * FROM ${merge_spec} ORDER BY x")"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON ${CLICKHOUSE_DATABASE}.* FROM ${user_name}"
grep -q "Either there is no database" <<<"$(error_as_user "DESCRIBE TABLE ${merge_spec}")"
$CLICKHOUSE_CLIENT --query "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.${table1} TO ${user_name}"
grep -q "it's necessary to have the grant SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.${table1}" <<<"$(error_as_user "DESCRIBE TABLE ${merge_spec}")"
$CLICKHOUSE_CLIENT --query "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.${table1} TO ${user_name}"
[ "$(query_as_user "DESCRIBE TABLE ${merge_spec}")" = $'x\tUInt32\t\t\t\t\t' ]
echo "OK"

echo "view_if_permitted"
[ "$($CLICKHOUSE_CLIENT --query "SELECT * FROM viewIfPermitted(SELECT * FROM ${table1} ELSE null('x UInt32'))")" = "1" ]
expected="requires a SELECT query with the result columns matching a table function after 'ELSE'"
grep -q "$expected" <<<"$($CLICKHOUSE_CLIENT --query "SELECT * FROM viewIfPermitted(SELECT * FROM ${table1} ELSE null('x Int32'))" 2>&1 || true)"
grep -q "$expected" <<<"$($CLICKHOUSE_CLIENT --query "SELECT * FROM viewIfPermitted(SELECT * FROM ${table1} ELSE null('y UInt32'))" 2>&1 || true)"
[ "$(query_as_user "SELECT * FROM viewIfPermitted(SELECT * FROM ${table1} ELSE null('x UInt32'))")" = "" ]
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${table1} TO ${user_name}"
[ "$(query_as_user "SELECT * FROM viewIfPermitted(SELECT * FROM ${table1} ELSE null('x UInt32'))")" = "1" ]
echo "OK"
