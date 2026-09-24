#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The object storage counterpart of `05139_file_split_on_write_keeps_foreign_names_after_setting_change`:
# a truncating insert with `s3_create_new_file_on_insert` back at 0 deletes the objects the table has
# written itself, and not the keys it had to step over.

PREFIX="05140_split_foreign_names/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers, and a new object is started as soon as 1000 bytes are written,
# so the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 1000"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/data.1.tsv', format=TSV) SELECT 111111 SETTINGS s3_truncate_on_insert = 1;
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/data.3.tsv', format=TSV) SELECT 333333 SETTINGS s3_truncate_on_insert = 1;

    CREATE TABLE test_05140 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);

    SELECT '--- A large insert skips the taken keys';
    INSERT INTO test_05140 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, s3_create_new_file_on_insert = 1;
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;

    SELECT '--- A smaller truncating insert with s3_create_new_file_on_insert back at 0';
    INSERT INTO test_05140 SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1, s3_create_new_file_on_insert = 0;
    SELECT count(), min(x), max(x) FROM test_05140;

    SELECT '--- The objects of the previous insert are deleted, the keys it did not own are intact';
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
    SELECT * FROM s3(s3_conn, filename='${PREFIX}/data.1.tsv', format=TSV, structure='x UInt64');
    SELECT * FROM s3(s3_conn, filename='${PREFIX}/data.3.tsv', format=TSV, structure='x UInt64');

    DROP TABLE test_05140;
"
