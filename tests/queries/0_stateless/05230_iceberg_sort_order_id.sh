#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_SORTED="sorted_${CLICKHOUSE_DATABASE}"
TABLE_UNSORTED="unsorted_${CLICKHOUSE_DATABASE}"
PATH_SORTED="${USER_FILES_PATH}/${TABLE_SORTED}/"
PATH_UNSORTED="${USER_FILES_PATH}/${TABLE_UNSORTED}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${TABLE_SORTED};
        DROP TABLE IF EXISTS ${TABLE_UNSORTED}"
    rm -rf "${PATH_SORTED}" "${PATH_UNSORTED}"
}
trap cleanup EXIT

# Order id 0 is reserved for the unsorted order, so a table with an ORDER BY must not use it:
# `default-sort-order-id = 0` advertises the table as unsorted to every other engine.
show_sort_orders()
{
    local label=$1
    local table_path=$2

    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            '${label}',
            JSONExtractInt(json, 'default-sort-order-id') AS default_id,
            JSONExtractString(json, 'sort-orders') AS orders
        FROM (SELECT * FROM file('${table_path}metadata/v1.metadata.json', JSONAsString, 'json String'))"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_SORTED} (n UInt64, s String)
    ENGINE = IcebergLocal('${PATH_SORTED}', 'Parquet')
    ORDER BY (n)"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_UNSORTED} (n UInt64, s String)
    ENGINE = IcebergLocal('${PATH_UNSORTED}', 'Parquet')"

show_sort_orders sorted "${PATH_SORTED}"
show_sort_orders unsorted "${PATH_UNSORTED}"

# The sort order must still be found by its own `default-sort-order-id` when the table is read back.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE_SORTED} VALUES (2, 'b'), (1, 'a')"

${CLICKHOUSE_CLIENT} --query "SELECT 'sorting_key', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE_SORTED}'"
${CLICKHOUSE_CLIENT} --query "SELECT 'rows', n, s FROM ${TABLE_SORTED} ORDER BY n"
