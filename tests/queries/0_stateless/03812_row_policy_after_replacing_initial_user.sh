#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user_name="user_${CLICKHOUSE_DATABASE}"
row_policy_a="policy_a_${CLICKHOUSE_DATABASE}"
row_policy_b="policy_b_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user_name}"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY IF EXISTS ${row_policy_a}, ${row_policy_b} ON test"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test(x Int64) ENGINE=Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO test SELECT number FROM numbers(10)"

$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY ${row_policy_a} ON test FOR SELECT USING x % 2 = 0"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY ${row_policy_b} ON test FOR SELECT USING x % 2 = 1"

echo 'with row policy a:'
$CLICKHOUSE_CLIENT -q "CREATE USER ${user_name}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON test, REMOTE ON *.* TO ${user_name}"
$CLICKHOUSE_CLIENT -q "ALTER ROW POLICY ${row_policy_a} ON test TO ${user_name}"
$CLICKHOUSE_CLIENT --user ${user_name} -q "SELECT * FROM cluster('test_shard_localhost', currentDatabase(), test)"
$CLICKHOUSE_CLIENT -q "DROP USER ${user_name}"

echo 'with row policy b:'
$CLICKHOUSE_CLIENT -q "CREATE USER ${user_name}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON test, REMOTE ON *.* TO ${user_name}"
$CLICKHOUSE_CLIENT -q "ALTER ROW POLICY ${row_policy_b} ON test TO ${user_name}"
$CLICKHOUSE_CLIENT --user ${user_name} -q "SELECT * FROM cluster('test_shard_localhost', currentDatabase(), test)"
$CLICKHOUSE_CLIENT -q "DROP USER ${user_name}"

$CLICKHOUSE_CLIENT -q "DROP TABLE test"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY ${row_policy_a}, ${row_policy_b} ON test"
