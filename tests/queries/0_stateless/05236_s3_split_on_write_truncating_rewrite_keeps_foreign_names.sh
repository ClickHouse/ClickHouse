#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The object storage counterpart of `05235_file_split_on_write_truncating_rewrite_keeps_foreign_names`:
# a truncating insert that is itself split by size must step over the numbered keys it does not own
# when `s3_create_new_file_on_insert` declares the numbered namespace shared.

PREFIX="05236_split_truncating_rewrite_foreign_names/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers, and a new object is started as soon as 1000 bytes are written,
# so the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 1000, s3_create_new_file_on_insert = 1"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/data.1.tsv', format=TSV) SELECT 111111 SETTINGS s3_truncate_on_insert = 1;
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/data.3.tsv', format=TSV) SELECT 333333 SETTINGS s3_truncate_on_insert = 1;

    CREATE TABLE test_05236 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);

    SELECT '--- A large insert skips the taken keys';
    INSERT INTO test_05236 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT count(), min(x), max(x) FROM test_05236;

    SELECT '--- A truncating rewrite that needs the numbered keys as well';
    INSERT INTO test_05236 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
    SELECT count(), min(x), max(x) FROM test_05236;

    SELECT '--- The keys the rewrite does not own are intact';
    SELECT * FROM s3(s3_conn, filename='${PREFIX}/data.1.tsv', format=TSV, structure='x UInt64');
    SELECT * FROM s3(s3_conn, filename='${PREFIX}/data.3.tsv', format=TSV, structure='x UInt64');

    SELECT '--- The objects of the previous insert are deleted, and the rewrite has stepped over the foreign keys';
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;

    DROP TABLE test_05236;
"
