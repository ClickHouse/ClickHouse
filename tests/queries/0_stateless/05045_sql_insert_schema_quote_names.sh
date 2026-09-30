#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

table_name="sql_insert_schema_quote_names_05045"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table_name}" >/dev/null 2>&1 || true
}

trap cleanup EXIT
cleanup

dump="$(${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt8(1) AS \`a b\`
    FORMAT SQLInsert
    SETTINGS
        output_format_sql_insert_include_table_schema = 1,
        output_format_sql_insert_quote_names = 0,
        output_format_sql_insert_table_name = '${table_name}'
")"

printf '%s\n' "${dump}"
printf '%s\n' "${dump}" | ${CLICKHOUSE_CLIENT} --multiquery

${CLICKHOUSE_CLIENT} --query "
    SELECT name, type
    FROM system.columns
    WHERE database = currentDatabase() AND table = '${table_name}'
    ORDER BY position
    FORMAT TSV
"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${table_name} FORMAT TSV"
