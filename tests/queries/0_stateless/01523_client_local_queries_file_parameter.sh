#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "SELECT 1;" > _01523_client_local_queries_file_parameter_tmp.sql
$CLICKHOUSE_CLIENT --queries-file=_01523_client_local_queries_file_parameter_tmp.sql 2>&1

echo "CREATE TABLE _01523_test(value Int32) ENGINE=Log;
INSERT INTO _01523_test
    VALUES (1), (2), (3);
SELECT * FROM _01523_test;
DROP TABLE _01523_test;" > _01523_client_local_queries_file_parameter_tmp.sql
$CLICKHOUSE_CLIENT --queries-file=_01523_client_local_queries_file_parameter_tmp.sql 2>&1

echo "CREATE TABLE _01523_test (a Int64, b Int64) ENGINE = File(CSV, stdin);
SELECT a, b FROM _01523_test;
DROP TABLE _01523_test;" > _01523_client_local_queries_file_parameter_tmp.sql

echo -e "1,2\n3,4" | $CLICKHOUSE_LOCAL --queries-file=_01523_client_local_queries_file_parameter_tmp.sql 2>&1

rm _01523_client_local_queries_file_parameter_tmp.sql
