#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="04947_split_on_write/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers, and a new object is started as soon as 1000 bytes are written,
# so the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 1000"

echo '--- Split into numbered objects'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/split/data.tsv', format=TSV, structure='x UInt64')
        SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT _file, count(), min(x), max(x) FROM s3(s3_conn, filename='${PREFIX}/split/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
    SELECT count(), sum(x) FROM s3(s3_conn, filename='${PREFIX}/split/data*.tsv', format=TSV, structure='x UInt64');
"

echo '--- The numbering continues from the number in the key of the first object'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/offset/data.5.tsv', format=TSV, structure='x UInt64')
        SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT _file, count() FROM s3(s3_conn, filename='${PREFIX}/offset/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
"

echo '--- Without splitting everything goes into a single object'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/single/data.tsv', format=TSV, structure='x UInt64')
        SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, s3_split_on_write_by_size_bytes = 0;
    SELECT uniqExact(_file), count() FROM s3(s3_conn, filename='${PREFIX}/single/data*.tsv', format=TSV, structure='x UInt64');
"

echo '--- All the split objects are visible for the S3 engine table'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/engine/data.tsv', format=TSV);
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT count(), sum(x), uniqExact(_file) FROM test;
"

echo '--- An exception if the key is already taken'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/exists/data.1.tsv', format=TSV, structure='x UInt64') SELECT 42;
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/exists/data.tsv', format=TSV, structure='x UInt64')
        SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo '--- The taken keys are skipped with s3_create_new_file_on_insert'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/skip/data.1.tsv', format=TSV, structure='x UInt64') SELECT 42;
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/skip/data.3.tsv', format=TSV, structure='x UInt64') SELECT 42;
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/skip/data.tsv', format=TSV, structure='x UInt64')
        SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, s3_create_new_file_on_insert = 1;
    SELECT _file, count() FROM s3(s3_conn, filename='${PREFIX}/skip/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
"

echo '--- Every partition is split on its own'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/partitioned/p_{_partition_id}.tsv', format=TSV, structure='p UInt64, x UInt64') PARTITION BY p
        SELECT number % 2 AS p, number AS x FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT p, count(), sum(x), uniqExact(_file) FROM s3(s3_conn, filename='${PREFIX}/partitioned/p_*.tsv', format=TSV, structure='p UInt64, x UInt64') GROUP BY p ORDER BY p;
"
