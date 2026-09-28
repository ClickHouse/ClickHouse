#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The same as 05237, for a partitioned table: the keys of a partition are not in the list of the paths of
# the table, so an object that an insert has committed stays taken for the next inserts into the partition
# in the configuration of the table - the base key as well as the numbered ones. Otherwise, once the insert
# is over, only the object storage would tell that the key is taken, and not every S3 implementation reports
# a just written object yet.

PREFIX="05259_split_partitioned_concurrent_inserts/${CLICKHOUSE_DATABASE}"

# Every block is 20 numbers and a new object is started as soon as 40 bytes are written, so each
# insert rolls over into a numbered key of every partition on every block.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 20, min_insert_block_size_rows = 20, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 40, s3_create_new_file_on_insert = 1, function_sleep_max_microseconds_per_block = 10000000"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05259 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data_{_partition_id}.tsv', format=TSV) PARTITION BY x % 2;
"

echo '--- Inserts one after another'
for offset in 1000000 2000000; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO test_05259 SELECT number + ${offset} FROM numbers(100) SETTINGS ${SETTINGS}"
done

echo '--- Inserts at the same time'
for offset in 3000000 4000000; do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO test_05259 SELECT number + ${offset} FROM numbers(100) WHERE NOT sleepEachRow(0.02) SETTINGS ${SETTINGS};
    " &
done
wait

echo '--- Every row of every insert is there'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), uniqExact(x) FROM s3(s3_conn, filename='${PREFIX}/data_*.tsv', format=TSV, structure='x UInt64');
"

echo '--- No key holds the rows of two inserts or of two partitions'
${CLICKHOUSE_CLIENT} --query "
    SELECT max(inserts_in_one_object), max(partitions_in_one_object) FROM
    (
        SELECT uniqExact(intDiv(x, 1000000)) AS inserts_in_one_object, uniqExact(x % 2) AS partitions_in_one_object
        FROM s3(s3_conn, filename='${PREFIX}/data_*.tsv', format=TSV, structure='x UInt64')
        GROUP BY _file
    );
"

echo '--- A truncating insert split by size rewrites every partition, the numbered keys included'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_05259 SELECT number + 5000000 FROM numbers(60)
    SETTINGS max_block_size = 20, min_insert_block_size_rows = 20, min_insert_block_size_bytes = 0,
        s3_split_on_write_by_size_bytes = 40, s3_truncate_on_insert = 1;
"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), uniqExact(x), min(x), max(x) FROM s3(s3_conn, filename='${PREFIX}/data_*.tsv', format=TSV, structure='x UInt64');
"

echo '--- The next insert steps aside from the rewritten keys'
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_05259 SELECT number + 6000000 FROM numbers(100) SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "
    SELECT intDiv(x, 1000000) AS insert, count(), uniqExact(x)
    FROM s3(s3_conn, filename='${PREFIX}/data_*.tsv', format=TSV, structure='x UInt64')
    GROUP BY insert ORDER BY insert;
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_05259"
