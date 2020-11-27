#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

echo 'DROP TABLE IF EXISTS insert_fewer_columns'                            | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'CREATE TABLE insert_fewer_columns (a UInt8, b UInt8) ENGINE = Memory' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'INSERT INTO insert_fewer_columns (a) VALUES (1), (2)'                 | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'SELECT * FROM insert_fewer_columns'                                   | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
echo 'DROP TABLE insert_fewer_columns'                                      | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @-
