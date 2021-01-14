#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

verify_sql="SELECT COUNT(1)
FROM
(SELECT
    SUM(IF(metric = 'Parts', value, 0)) AS Parts,
    SUM(IF(metric = 'PartsActive', value, 0)) AS PartsActive,
    SUM(IF(metric = 'PartsInactive', value, 0)) AS PartsInactive
FROM system.metrics) as a INNER JOIN
(SELECT
    toInt64(SUM(1)) AS Parts,
    toInt64(SUM(IF(active = 1, 1, 0))) AS PartsActive,
    toInt64(SUM(IF(active = 0, 1, 0))) AS PartsInactive
FROM system.parts
) as b USING (Parts,PartsActive,PartsInactive)"

# The query is not atomic - it can compare states between system.parts and system.metrics from different points in time.
# So, there is inherent race condition. But it should get expected result eventually.
# In case of test failure, this code will do infinite loop and timeout.
verify()
{
    while true
    do
        result=$( $CLICKHOUSE_CLIENT -m --query="$verify_sql" )
        [ "$result" = "1" ] && break;
    done
    echo 1
}

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_table"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_table(data Date) ENGINE = MergeTree  PARTITION BY toYear(data) ORDER BY data;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-01')"
verify

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-02')"
verify

$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE test_table FINAL"
verify

$CLICKHOUSE_CLIENT --query="DROP TABLE test_table"
verify
