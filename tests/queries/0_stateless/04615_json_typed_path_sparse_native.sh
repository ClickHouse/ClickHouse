#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_TEST_UNIQUE_NAME}"
compressed_table="${table}_compressed"
wrapped_table="${table}_wrapped"
wrapped_type="Nullable(Tuple(a Nullable(String), j JSON(x Nullable(String), max_dynamic_paths = 0)))"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS ${table};
    CREATE TABLE ${table}
    (
        j JSON(x Nullable(String), y String, max_dynamic_paths = 0)
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        ratio_of_defaults_for_sparse_serialization = 0.5,
        serialization_info_version = 'with_subcolumns',
        nullable_serialization_version = 'allow_sparse';
    INSERT INTO ${table}
    SELECT CAST(
        if(
            number = 0,
            '{\"x\":\"value\",\"y\":\"dense\"}',
            '{\"x\":null,\"y\":\"dense\"}'),
        'JSON(x Nullable(String), y String, max_dynamic_paths = 0)')
    FROM numbers(100);
"

${CLICKHOUSE_CLIENT} --query "SELECT j FROM ${table} FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query \
        "SELECT count(), countIf(j.x = 'value'), countIf(j.y = 'dense') FROM table"

${CLICKHOUSE_CLIENT} --query \
    "WITH (SELECT j FROM ${table} LIMIT 1) AS s SELECT s FROM numbers(10) FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query \
        "SELECT count(), countIf(s.x = 'value'), countIf(s.y = 'dense') FROM table"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${compressed_table} AS ${table} ENGINE = Memory SETTINGS compress = 1;
    INSERT INTO ${compressed_table} SELECT * FROM ${table};
"

${CLICKHOUSE_CLIENT} --query "SELECT j FROM ${compressed_table} FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query \
        "SELECT count(), countIf(j.x = 'value'), countIf(j.y = 'dense') FROM table"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE ${table};
    DROP TABLE ${compressed_table};
    SET enable_nullable_tuple_type = 1;
    CREATE TABLE ${wrapped_table}
    (
        v ${wrapped_type}
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        ratio_of_defaults_for_sparse_serialization = 0.5,
        serialization_info_version = 'with_subcolumns',
        nullable_serialization_version = 'allow_sparse';
    INSERT INTO ${wrapped_table}
    SELECT CAST(
        (if(number = 0, 'value', NULL), if(number = 0, '{\"x\":\"value\"}', '{}')),
        '${wrapped_type}')
    FROM numbers(100);
    SELECT dumpColumnStructure(v) LIKE '%Sparse%' FROM ${wrapped_table} LIMIT 1;
"

${CLICKHOUSE_CLIENT} --query "SELECT v FROM ${wrapped_table} FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --enable_nullable_tuple_type=1 --input-format Native --query \
        "SELECT dumpColumnStructure(v) LIKE '%Sparse%', v.a, v.j.x FROM table LIMIT 2"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${wrapped_table}"
