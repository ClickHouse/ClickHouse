#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_insert_sparse_columns;
    CREATE TABLE t_insert_sparse_columns (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
    SYSTEM STOP MERGES t_insert_sparse_columns;
"

${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d 'INSERT INTO t_insert_sparse_columns FORMAT JSONEachRow {"id": 1} {"id": 2} {"id": 3} {"id": 4} {"id": 5}'
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d 'INSERT INTO t_insert_sparse_columns FORMAT JSONEachRow {"id": 6} {"id": 7} {"id": 8} {"id": 9} {"id": 10}'
${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d 'INSERT INTO t_insert_sparse_columns FORMAT JSONEachRow {"id": 11, "v": 100} {"id": 12, "v": 200} {"id": 13, "v": 300} {"id": 14, "v": 400} {"id": 15, "v": 500}'

$CLICKHOUSE_CLIENT --query "
    SELECT * FROM t_insert_sparse_columns ORDER BY id;

    SELECT name, column, serialization_kind FROM system.parts_columns
    WHERE table = 't_insert_sparse_columns' AND database = currentDatabase() AND active
    ORDER BY name, column;

    DROP TABLE t_insert_sparse_columns;
"
