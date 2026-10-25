#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `TRUNCATE TABLE` empties an `S3` table that has been written by an insert split by size: the objects of the
# previous inserts are deleted and the numbered keys are forgotten, so that the table does not go on planning
# reads of objects that do not exist anymore, and the next split insert starts the numbering over.
# (The truncated table itself is not read here: its base object is gone as well, and reading a missing
# base object is an error of the engine that has nothing to do with the splitting.)

PREFIX="05213_split_truncate_table/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers, and a new object is started as soon as 1000 bytes are written,
# so the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 1000"

${CLICKHOUSE_CLIENT} --query "
    SELECT '--- A split insert';
    CREATE TABLE test_05213 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
    INSERT INTO test_05213 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT count(), min(x), max(x) FROM test_05213;
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;

    SELECT '--- The table is truncated: all the objects are gone';
    TRUNCATE TABLE test_05213;
    SELECT count() FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64');

    SELECT '--- The next split insert starts the numbering over';
    INSERT INTO test_05213 SELECT number FROM numbers(1000, 500) SETTINGS ${SETTINGS};
    SELECT count(), min(x), max(x) FROM test_05213;
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
    DROP TABLE test_05213;
"
