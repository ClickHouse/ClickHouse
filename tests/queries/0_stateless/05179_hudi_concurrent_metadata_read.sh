#!/usr/bin/env bash
# Tags: no-fasttest

# Concurrent readers of one `Hudi` table. The metadata object of a data lake table is shared by the
# queries that run on it, and `HudiMetadata` filled its file list on first use with no
# synchronization: two queries that reach an unlisted table together raced on that list. A data race
# is not visible in query output, so a regression shows up as a sanitizer report rather than as a
# diff against the reference; the row counts are asserted so that a stream dying early is noticed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="05179_hudi_concurrent/${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="t_${CLICKHOUSE_DATABASE}"
trap '$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"' EXIT

# A Hudi data file is `[FileId]_[FileWriteToken]_[Timestamp].[ext]`; the reader keeps the newest
# write token of every file id.
for file_id in 0000 0001 0002
do
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/${file_id}_1-0-1_20260101000000.parquet')
        SETTINGS s3_truncate_on_insert = 1
        SELECT number AS a FROM numbers(10)
    "
done

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE} ENGINE = Hudi(s3_conn, filename='${TABLE_PATH}/')
"

# Each stream reads the table repeatedly, so that several of them list the files at the same time.
read_stream() {
    for _ in {1..10}
    do
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${TABLE}"
    done
}

for _ in {1..4}
do
    read_stream &
done
wait

echo 'every read saw the whole table'
