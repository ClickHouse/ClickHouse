#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "SELECT 1;" > 01523_client_local_queries_file_parameter_tmp.sql
$CLICKHOUSE_CLIENT --queries-file=01523_client_local_queries_file_parameter_tmp.sql 2>&1

echo "CREATE TABLE test_01523(value Int32) ENGINE=Log;
INSERT INTO test_01523 
    VALUES (1), (2), (3);
SELECT * FROM test_01523;
DROP TABLE test_01523;" > 01523_client_local_queries_file_parameter_tmp.sql
$CLICKHOUSE_CLIENT --queries-file=01523_client_local_queries_file_parameter_tmp.sql 2>&1

echo "CREATE TABLE test_01523 (a Int64, b Int64) ENGINE = File(CSV, stdin);
SELECT a, b FROM test_01523;
DROP TABLE test_01523;" > 01523_client_local_queries_file_parameter_tmp.sql

echo -e "1,2\n3,4" | $CLICKHOUSE_LOCAL --queries-file=01523_client_local_queries_file_parameter_tmp.sql 2>&1

rm 01523_client_local_queries_file_parameter_tmp.sql
