#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t;"

$CLICKHOUSE_CLIENT --query "CREATE TABLE t (key UInt64) ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_parts_to_merge_at_once=5, merge_selector_base=1"

$CLICKHOUSE_CLIENT --query "INSERT INTO t VALUES (1)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t VALUES (2);"

# doesn't make test flaky
sleep 1

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE active and database = currentDatabase() and table = 't'"

$CLICKHOUSE_CLIENT --query "INSERT INTO t VALUES (3)"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE active and database = currentDatabase() and table = 't'"

$CLICKHOUSE_CLIENT --query "INSERT INTO t VALUES (4)"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE active and database = currentDatabase() and table = 't'"

$CLICKHOUSE_CLIENT --query "INSERT INTO t VALUES (5)"

counter=0 retries=60

while [[ $counter -lt $retries ]]; do
    result=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE active and database = currentDatabase() and table = 't'")
    if [ "$result" -eq "1" ];then
        break;
    fi
    sleep 0.5
    counter=$((counter + 1))
done

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE active and database = currentDatabase() and table = 't'"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t"
