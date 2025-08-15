#!/usr/bin/env bash
# Tags: no-fasttest, use-hdfs

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


TCP_PORT=$($CLICKHOUSE_CLIENT -q "SELECT tcpPort()")
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('hdfs://localhost:$TCP_PORT/data.csv', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "HDFS_ERROR" && echo 'OK' || echo 'FAIL';
