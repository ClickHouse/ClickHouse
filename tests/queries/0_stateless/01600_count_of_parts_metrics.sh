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

verify(){
for _ in $(seq 1 10)
do
result=$( $CLICKHOUSE_CLIENT -m --query="$verify_sql" )
if [ "$result" = "1" ];then
    echo 1
    return
fi 
done
echo 0  
}

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_table" 2>/dev/null
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_table(data Date) ENGINE = MergeTree  PARTITION BY toYear(data) ORDER BY data;" 2>/dev/null

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-01')" 2>/dev/null
verify

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-02')" 2>/dev/null
verify

$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE test_table FINAL" 2>/dev/null
verify

$CLICKHOUSE_CLIENT --query="DROP TABLE test_table" 2>/dev/null
verify
