#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo 'DROP TABLE IF EXISTS test.insert_fewer_columns'                            | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL} -d @-
echo 'CREATE TABLE test.insert_fewer_columns (a UInt8, b UInt8) ENGINE = Memory' | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL} -d @-
echo 'INSERT INTO test.insert_fewer_columns (a) VALUES (1), (2)'                 | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL} -d @-
echo 'SELECT * FROM test.insert_fewer_columns'                                   | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL} -d @-
