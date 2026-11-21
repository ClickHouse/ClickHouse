#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two inserts into the same object storage table run at the same time. A key that one of them has
# generated is published for the readers only after its object is committed, and the object storage
# does not have it before that either, so the key generation of the other insert has to see the
# reservation - otherwise both write the same key and one of them loses all of its rows.

PREFIX="05237_split_concurrent_inserts/${CLICKHOUSE_DATABASE}"

# Every block is 20 numbers and a new object is started as soon as 40 bytes are written, so each
# insert rolls over into a numbered key on every block. The rows are produced slowly, so that the
# two inserts are generating their keys at the same time.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 20, min_insert_block_size_rows = 20, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 40, s3_create_new_file_on_insert = 1, function_sleep_max_microseconds_per_block = 10000000"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV) SELECT 0 SETTINGS s3_truncate_on_insert = 1;
    CREATE TABLE test_05237 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
"

for offset in 1000000 2000000; do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO test_05237 SELECT number + ${offset} FROM numbers(100) WHERE NOT sleepEachRow(0.02) SETTINGS ${SETTINGS};
    " &
done
wait

echo '--- Every row of both inserts is there'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), uniqExact(x) FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64');
"

echo '--- No key holds the rows of both inserts'
${CLICKHOUSE_CLIENT} --query "
    SELECT max(inserts_in_one_object) FROM
    (
        SELECT uniqExact(intDiv(x, 1000000)) AS inserts_in_one_object
        FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64')
        WHERE x > 0
        GROUP BY _file
    );
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_05237"
