#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="05046_split_reinsert/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers, and a new object is started as soon as 1000 bytes are written,
# so the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 1000"

echo '--- A truncating insert into the same table starts the numbering over and overwrites the split objects'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05046_truncate (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/truncate/data.tsv', format=TSV);
    INSERT INTO test_05046_truncate SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    INSERT INTO test_05046_truncate SELECT number FROM numbers(500) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
    SELECT _file, count(), min(x), max(x) FROM test_05046_truncate GROUP BY _file ORDER BY _file;
    SELECT count(), sum(x) FROM test_05046_truncate;
    DROP TABLE test_05046_truncate;
"

echo '--- A second insert continues after the taken keys with s3_create_new_file_on_insert'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05046_multiple (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/multiple/data.tsv', format=TSV);
    INSERT INTO test_05046_multiple SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    INSERT INTO test_05046_multiple SELECT number FROM numbers(500) SETTINGS ${SETTINGS}, s3_create_new_file_on_insert = 1;
    SELECT _file, count() FROM test_05046_multiple GROUP BY _file ORDER BY _file;
    SELECT count(), sum(x) FROM test_05046_multiple;
    DROP TABLE test_05046_multiple;
"

echo '--- An exception if the base key is already taken'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05046_exists (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/exists/data.tsv', format=TSV);
    INSERT INTO test_05046_exists SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    INSERT INTO test_05046_exists SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_05046_exists"
