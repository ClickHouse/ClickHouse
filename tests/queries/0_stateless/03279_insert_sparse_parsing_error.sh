#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_insert_sparse_columns;

    CREATE TABLE t_insert_sparse_columns (a UInt64, b UInt64, c UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, enable_block_number_column = 0, enable_block_offset_column = 0;

    SYSTEM STOP MERGES t_insert_sparse_columns;
"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_insert_sparse_columns+FORMAT+CSV" --data-binary @- <<EOF
1, 0, 0
2, 0, 0
EOF

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_insert_sparse_columns+FORMAT+CSV" --data-binary @- <<EOF 2>&1 | grep -o "Code: 27"
3, 0
4, 0
EOF

$CLICKHOUSE_CLIENT --query "
    SELECT * FROM t_insert_sparse_columns;
    SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_insert_sparse_columns' AND active ORDER BY column, serialization_kind;
    DROP TABLE IF EXISTS t_insert_sparse_columns;
"
