#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP TABLE IF EXISTS ${table};
    CREATE TABLE ${table}
    (
        j JSON(x Nullable(String), y String, max_dynamic_paths = 1)
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
            '{\"x\":\"value\",\"y\":\"dense\",\"dynamic\":1,\"shared\":\"value\"}',
            '{\"x\":null,\"y\":\"dense\",\"dynamic\":1,\"shared\":\"value\"}'),
        'JSON(x Nullable(String), y String, max_dynamic_paths = 1)')
    FROM numbers(100);
"

${CLICKHOUSE_CLIENT} --query "SELECT j FROM ${table} FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query \
        "SELECT count(), countIf(j.x = 'value'), countIf(j.y = 'dense'), countIf(length(JSONSharedDataPaths(j)) > 0) FROM table"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+j+FROM+${table}+FORMAT+Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query \
        "SELECT count(), countIf(j.x = 'value'), countIf(j.y = 'dense'), countIf(length(JSONSharedDataPaths(j)) > 0) FROM table"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP TABLE IF EXISTS ${table}_remote;
    CREATE TABLE ${table}_remote AS ${table};
    INSERT INTO ${table}_remote SELECT * FROM remote('127.0.0.1', currentDatabase(), '${table}');
    SELECT count(), countIf(j.x = 'value'), countIf(j.y = 'dense'), countIf(length(JSONSharedDataPaths(j)) > 0) FROM ${table}_remote;
    DROP TABLE ${table}_remote;
    DROP TABLE ${table};
"
