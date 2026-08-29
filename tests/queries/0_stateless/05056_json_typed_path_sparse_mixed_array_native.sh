#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_TEST_UNIQUE_NAME}"
type="Array(Tuple(a Nullable(String), o JSON(x Nullable(String), max_dynamic_paths = 0)))"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS ${table};
    CREATE TABLE ${table} (id UInt64, arr ${type})
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS
        ratio_of_defaults_for_sparse_serialization = 0.5,
        serialization_info_version = 'with_subcolumns',
        nullable_serialization_version = 'allow_sparse';
    INSERT INTO ${table}
    SELECT number, CAST([
        (if(number = 0, 'value', NULL), if(number = 0, '{\"x\":\"value\"}', '{}'))
    ], '${type}')
    FROM numbers(100);
"

# The non-JSON tuple element is sparse in the source part.
${CLICKHOUSE_CLIENT} --query \
    "SELECT dumpColumnStructure(arr) LIKE '%Sparse%' FROM ${table} LIMIT 1"

# Materializing the unsupported typed JSON path must not densify its sparse sibling.
${CLICKHOUSE_CLIENT} --query "SELECT arr FROM ${table} ORDER BY id FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query \
        "SELECT dumpColumnStructure(arr) LIKE '%Sparse%', arr.a, arr.o.x FROM table LIMIT 2"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
