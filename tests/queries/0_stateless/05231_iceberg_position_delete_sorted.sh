#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

# The rows of a position delete file must be sorted by `file_path` and then `pos`. The delete rows of
# one mutation come from many data files and arrive interleaved, so an unsorted writer fails this.
check()
{
    local label=$1

    for delete_file in $(find "${TABLE_PATH}data" -name '*-deletes.parquet' -type f); do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT
                '${label}',
                count() AS rows,
                groupArray((file_path, pos)) = arraySort(groupArray((file_path, pos))) AS sorted
            FROM file('${delete_file}', Parquet)
            SETTINGS input_format_parquet_preserve_order = 1, max_threads = 1, max_block_size = 64"
    done | sort
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (n UInt64, s String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} SELECT number, toString(number) FROM numbers(5000)
    SETTINGS iceberg_insert_max_rows_in_data_file = 100, max_insert_threads = 1,
             max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "
    DELETE FROM ${TABLE} WHERE n % 3 = 0 SETTINGS max_threads = 8, max_block_size = 64"

check delete

# An UPDATE writes position deletes for the rows it replaces through the same path.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "
    ALTER TABLE ${TABLE} UPDATE s = 'x' WHERE n % 5 = 0 SETTINGS max_threads = 8, max_block_size = 64"

check update

${CLICKHOUSE_CLIENT} --query "SELECT 'rows', count(), countIf(s = 'x') FROM ${TABLE}"
