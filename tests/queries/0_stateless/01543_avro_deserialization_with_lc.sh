#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT --multiquery --query "
SET allow_suspicious_low_cardinality_types=1;
CREATE TABLE IF NOT EXISTS test_01543 (value LowCardinality(String), value2 LowCardinality(UInt64)) ENGINE=Memory();
"

$CLICKHOUSE_CLIENT --query "INSERT INTO test_01543 SELECT toString(number), number FROM numbers(10)"

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_01543 FORMAT Avro" |
    $CLICKHOUSE_CLIENT -q "INSERT INTO test_01543 FORMAT Avro";

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_01543";

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_01543"

$CLICKHOUSE_CLIENT --query "SELECT number % 2 ? number: NULL as x from numbers(10) FORMAT Avro" > $USER_FILES_PATH/test_01543.avro

$CLICKHOUSE_CLIENT --query "SELECT * FROM file('test_01543.avro', 'Avro', 'x LowCardinality(Nullable(UInt64))')"

rm $USER_FILES_PATH/test_01543.avro
