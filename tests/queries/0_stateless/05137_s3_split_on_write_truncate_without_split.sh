#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="05137_split_truncate_without_split/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers, and a new object is started as soon as 1000 bytes are written,
# so the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0"

echo '--- A large split insert, then a truncating rewrite with the splitting turned off'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05137 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
    INSERT INTO test_05137 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, s3_split_on_write_by_size_bytes = 1000;
    INSERT INTO test_05137 SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, s3_split_on_write_by_size_bytes = 0, s3_truncate_on_insert = 1;
    SELECT 'engine table', count(), min(x), max(x) FROM test_05137;
    SELECT 'wildcard', count(), min(x), max(x) FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64');
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
    DROP TABLE test_05137;
"
