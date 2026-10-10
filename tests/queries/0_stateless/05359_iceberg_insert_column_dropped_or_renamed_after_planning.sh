#!/usr/bin/env bash
# Tags: no-fasttest

# An INSERT that was planned before a column was dropped or renamed must fail with the retryable
# "metadata changed" error and leave the table unchanged. Reading the table through a remembered older
# metadata file gives the old structure deterministically, which is the same divergence a concurrent
# ALTER produces.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL="${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0"

for alter in "DROP COLUMN note" "RENAME COLUMN note TO comment"
do
    TABLE_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
    TABLE_PATH="${TABLE_DIR}/t/"
    rm -rf "${TABLE_DIR}"
    mkdir -p "${TABLE_PATH}"

    ${LOCAL} --query "
        CREATE TABLE t (number Int32, note Nullable(String)) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
        INSERT INTO t SELECT toInt32(number), 'a' FROM numbers(3);
    "

    PIN="metadata/$(basename "$(ls "${TABLE_PATH}metadata"/*.metadata.json | sort -V | tail -1)")"
    ${LOCAL} --query "
        CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
        ALTER TABLE t ${alter};
    "

    echo "${alter}"
    ${LOCAL} --query "
        CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
            SETTINGS iceberg_metadata_file_path = '${PIN}';
        INSERT INTO t SELECT toInt32(number + 10), 'b' FROM numbers(3);
    " 2>&1 | grep -o -m1 "Metadata changed during write operation, try again"

    ${LOCAL} --query "
        CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
        SELECT 'rows', arraySort(groupArray(number)) FROM t;
    "

    rm -rf "${TABLE_DIR}"
done
