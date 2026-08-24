#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

filename="test_${CLICKHOUSE_DATABASE}_${RANDOM}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_s3_race"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_s3_race (u UInt64) ENGINE = S3(s3_conn, filename='$filename', format='CSV')"
$CLICKHOUSE_CLIENT --s3_truncate_on_insert 1 --query "INSERT INTO test_s3_race VALUES (1)"

$CLICKHOUSE_BENCHMARK -i 100 -c 4 <<< "SELECT * FROM test_s3_race" >/dev/null 2>&1
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_s3_race"
echo "OK"
