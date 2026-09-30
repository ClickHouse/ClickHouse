#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

# Covers https://github.com/ClickHouse/ClickHouse/issues/84736.
table_name="sql_insert_schema_04657"
empty_table_name="sql_insert_empty_schema_04657"
no_names_table_name="sql_insert_no_names_04657"
unquoted_table_name="sql_insert_unquoted_04657"
escaped_name_table_name="sql_insert_escaped_name_04657"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${table_name};
        DROP TABLE IF EXISTS ${empty_table_name};
        DROP TABLE IF EXISTS ${no_names_table_name};
        DROP TABLE IF EXISTS ${unquoted_table_name};
        DROP TABLE IF EXISTS ${escaped_name_table_name};
    " >/dev/null 2>&1 || true
}

trap cleanup EXIT
cleanup

echo "default output"
${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt8(1) AS x
    FORMAT SQLInsert
    SETTINGS output_format_sql_insert_table_name = 'plain_04657'
"

if incompatible_output="$(${CLICKHOUSE_CLIENT} --query "
    SELECT 1
    FORMAT SQLInsert
    SETTINGS
        output_format_sql_insert_include_table_schema = 1,
        output_format_sql_insert_use_replace = 1
" 2>&1)"
then
    echo "incompatible settings unexpectedly accepted"
    exit 1
fi

if [[ "${incompatible_output}" != *"BAD_ARGUMENTS"* \
    || "${incompatible_output}" != *"output_format_sql_insert_include_table_schema"* \
    || "${incompatible_output}" != *"output_format_sql_insert_use_replace"* ]]
then
    echo "unexpected error for incompatible settings"
    exit 1
fi

echo "incompatible settings rejected"

echo "schema output"
dump="$(${CLICKHOUSE_CLIENT} --query "
    SELECT
        number AS id,
        toString(number) AS value,
        CAST([number, NULL], 'Array(Nullable(UInt64))') AS values
    FROM numbers(2)
    FORMAT SQLInsert
    SETTINGS
        output_format_sql_insert_include_table_schema = 1,
        output_format_sql_insert_table_name = '${table_name}',
        output_format_sql_insert_max_batch_size = 1
")"
printf '%s\n' "${dump}"

printf '%s\n' "${dump}" | ${CLICKHOUSE_CLIENT} --multiquery

echo "replayed schema"
${CLICKHOUSE_CLIENT} --query "
    SELECT name, type
    FROM system.columns
    WHERE database = currentDatabase() AND table = '${table_name}'
    ORDER BY position
    FORMAT TSV
"

echo "replayed data"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${table_name} ORDER BY id FORMAT TSV"

echo "schema with INSERT column names disabled"
no_names_dump="$(${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt8(7) AS value
    FORMAT SQLInsert
    SETTINGS
        output_format_sql_insert_include_table_schema = 1,
        output_format_sql_insert_table_name = '${no_names_table_name}',
        output_format_sql_insert_include_column_names = 0
")"
printf '%s\n' "${no_names_dump}"
printf '%s\n' "${no_names_dump}" | ${CLICKHOUSE_CLIENT} --multiquery
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${no_names_table_name} FORMAT TSV"

echo "schema forces quoted column names"
unquoted_dump="$(${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt8(8) AS x
    FORMAT SQLInsert
    SETTINGS
        output_format_sql_insert_include_table_schema = 1,
        output_format_sql_insert_table_name = '${unquoted_table_name}',
        output_format_sql_insert_quote_names = 0
")"
printf '%s\n' "${unquoted_dump}"
printf '%s\n' "${unquoted_dump}" | ${CLICKHOUSE_CLIENT} --multiquery
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${unquoted_table_name} FORMAT TSV"

echo "schema with an escaped column name"
escaped_name_dump="$(${CLICKHOUSE_CLIENT} --query '
    SELECT toUInt8(9) AS `a\`b`
    FORMAT SQLInsert
    SETTINGS
        output_format_sql_insert_include_table_schema = 1,
        output_format_sql_insert_table_name = '\''sql_insert_escaped_name_04657'\''
')"
printf '%s\n' "${escaped_name_dump}"
printf '%s\n' "${escaped_name_dump}" | ${CLICKHOUSE_CLIENT} --multiquery
${CLICKHOUSE_CLIENT} --query "
    SELECT name, type
    FROM system.columns
    WHERE database = currentDatabase() AND table = '${escaped_name_table_name}'
    ORDER BY position
    FORMAT TSV
"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${escaped_name_table_name} FORMAT TSV"

echo "empty result schema"
empty_dump="$(${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt8(number) AS x
    FROM numbers(0)
    FORMAT SQLInsert
    SETTINGS
        output_format_sql_insert_include_table_schema = 1,
        output_format_sql_insert_table_name = '${empty_table_name}'
")"
printf '%s\n' "${empty_dump}"

printf '%s\n' "${empty_dump}" | ${CLICKHOUSE_CLIENT} --multiquery

echo "replayed empty table"
${CLICKHOUSE_CLIENT} --query "
    SELECT name, type
    FROM system.columns
    WHERE database = currentDatabase() AND table = '${empty_table_name}'
    ORDER BY position
    FORMAT TSV
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${empty_table_name}"
