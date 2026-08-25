#!/usr/bin/env bash
# https://github.com/ClickHouse/ClickHouse/issues/35818

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TEST_USER="${CLICKHOUSE_DATABASE}_usr_03827"
TEST_POLICY_DEFAULT="${CLICKHOUSE_DATABASE}_policy_default_03827"
TEST_POLICY="${CLICKHOUSE_DATABASE}_policy_03827"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_row_policy_partition"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_row_policy_no_partition"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE test_row_policy_partition
(
    app_id String,
    ts DateTime,
    event_id String
)
ENGINE = MergeTree()
PARTITION BY (app_id)
ORDER BY (ts)"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE test_row_policy_no_partition
(
    app_id String,
    ts DateTime,
    event_id String
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY (ts)"

$CLICKHOUSE_CLIENT --query "INSERT INTO test_row_policy_partition(app_id, ts, event_id) VALUES ('a', '2021-01-01', 'e1'), ('b', '2021-01-01', 'e2')"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_row_policy_no_partition(app_id, ts, event_id) VALUES ('a', '2021-01-01', 'e1'), ('b', '2021-01-01', 'e2')"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${TEST_USER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${TEST_USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${TEST_USER}"

$CLICKHOUSE_CLIENT --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY_DEFAULT} ON test_row_policy_partition, test_row_policy_no_partition"
$CLICKHOUSE_CLIENT --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON test_row_policy_partition, test_row_policy_no_partition"

$CLICKHOUSE_CLIENT --query "CREATE ROW POLICY ${TEST_POLICY_DEFAULT} ON test_row_policy_partition, test_row_policy_no_partition USING 1 TO ALL EXCEPT ${TEST_USER}"
$CLICKHOUSE_CLIENT --query "CREATE ROW POLICY ${TEST_POLICY} ON test_row_policy_partition, test_row_policy_no_partition USING app_id = 'a' TO ${TEST_USER}"

# Without partition column: should only see 'a'
$CLICKHOUSE_CLIENT --user="${TEST_USER}" --query "SELECT app_id FROM test_row_policy_no_partition"
$CLICKHOUSE_CLIENT --user="${TEST_USER}" --query "SELECT app_id FROM test_row_policy_no_partition GROUP BY app_id"

# With partition column: should also only see 'a'
$CLICKHOUSE_CLIENT --user="${TEST_USER}" --query "SELECT app_id FROM test_row_policy_partition"
$CLICKHOUSE_CLIENT --user="${TEST_USER}" --query "SELECT app_id FROM test_row_policy_partition GROUP BY app_id"

$CLICKHOUSE_CLIENT --query "DROP ROW POLICY ${TEST_POLICY_DEFAULT} ON test_row_policy_partition, test_row_policy_no_partition"
$CLICKHOUSE_CLIENT --query "DROP ROW POLICY ${TEST_POLICY} ON test_row_policy_partition, test_row_policy_no_partition"
$CLICKHOUSE_CLIENT --query "DROP USER ${TEST_USER}"
$CLICKHOUSE_CLIENT --query "DROP TABLE test_row_policy_partition"
$CLICKHOUSE_CLIENT --query "DROP TABLE test_row_policy_no_partition"
