#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two inserts into the same object storage table start at the same time while the prefix is still empty.
# Neither of them finds the starting key in the object storage, so nothing but the reservation of that key
# tells them apart: without it both write `data.tsv`, and the rows of one of them are lost - the whole insert
# when it never rolls over into a numbered key. The second insert has to step aside into `data.1.tsv`
# exactly like it steps aside from an object that already exists.

PREFIX="05239_split_concurrent_inserts_empty_prefix/${CLICKHOUSE_DATABASE}"

# The rows are produced slowly, so that the two inserts are choosing their keys at the same time.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 20, min_insert_block_size_rows = 20, min_insert_block_size_bytes = 0, s3_create_new_file_on_insert = 1, function_sleep_max_microseconds_per_block = 10000000"

function run_case()
{
    local split_on_write_by_size_bytes=$1
    local prefix="${PREFIX}/split_${split_on_write_by_size_bytes}"

    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS test_05239;
        CREATE TABLE test_05239 (x UInt64) ENGINE = S3(s3_conn, filename='${prefix}/data.tsv', format=TSV);
    "

    for offset in 1000000 2000000; do
        ${CLICKHOUSE_CLIENT} --query "
            INSERT INTO test_05239 SELECT number + ${offset} FROM numbers(100) WHERE NOT sleepEachRow(0.02)
            SETTINGS ${SETTINGS}, s3_split_on_write_by_size_bytes = ${split_on_write_by_size_bytes};
        " &
    done
    wait

    echo '--- Every row of both inserts is there'
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), uniqExact(x) FROM s3(s3_conn, filename='${prefix}/data*.tsv', format=TSV, structure='x UInt64');
    "

    echo '--- No key holds the rows of both inserts'
    ${CLICKHOUSE_CLIENT} --query "
        SELECT max(inserts_in_one_object) FROM
        (
            SELECT uniqExact(intDiv(x, 1000000)) AS inserts_in_one_object
            FROM s3(s3_conn, filename='${prefix}/data*.tsv', format=TSV, structure='x UInt64')
            GROUP BY _file
        );
    "

    echo '--- The table reads every object it has written'
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(x) FROM test_05239"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE test_05239"
}

echo '=== The inserts are not split by size: each of them writes exactly one object'
run_case 0

echo '--- The second insert stepped aside into the first numbered key'
${CLICKHOUSE_CLIENT} --query "
    SELECT _file, count() FROM s3(s3_conn, filename='${PREFIX}/split_0/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
"

echo '=== The inserts are split by size on every block'
run_case 40
