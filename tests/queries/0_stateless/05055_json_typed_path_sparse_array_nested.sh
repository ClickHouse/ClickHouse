#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS ${table};
    CREATE TABLE ${table}
    (
        id UInt64,
        arr Array(JSON(x Nullable(String), max_dynamic_paths = 0))
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS
        ratio_of_defaults_for_sparse_serialization = 0.5,
        serialization_info_version = 'with_subcolumns',
        nullable_serialization_version = 'allow_sparse';
    INSERT INTO ${table}
    SELECT
        number,
        CAST([if(number = 0, '{\"x\":\"value\"}', '{}')],
            'Array(JSON(x Nullable(String), max_dynamic_paths = 0))')
    FROM numbers(100);
"

${CLICKHOUSE_CLIENT} --query "
    SELECT estimateCompressionRatio(arr) > 0 FROM ${table};
    SELECT subcolumns.serializations[indexOf(subcolumns.names, 'x')]
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = '${table}'
        AND column = 'arr';
    SELECT arr.x FROM ${table} ORDER BY id LIMIT 2;
"

${CLICKHOUSE_CLIENT} --query "SELECT arr FROM ${table} ORDER BY id FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query \
        "SELECT count(), countIf(arr.x = ['value']), countIf(arr.x = [NULL]) FROM table"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
