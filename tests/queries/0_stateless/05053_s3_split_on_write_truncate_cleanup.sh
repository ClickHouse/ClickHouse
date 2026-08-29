#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="05053_split_truncate_cleanup/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers, and a new object is started as soon as 1000 bytes are written,
# so the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 1000"

echo '--- A large insert produces multiple numbered objects'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05053 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
    INSERT INTO test_05053 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    DROP TABLE test_05053;
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
"

echo '--- A smaller truncating insert deletes the leftovers of the previous one'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05053 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
    INSERT INTO test_05053 SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
    DROP TABLE test_05053;
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
"

echo '--- The stale rows are not visible for a wildcard reader'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), min(x), max(x) FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64');
"
