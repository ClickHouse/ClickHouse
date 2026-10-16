#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The same as 05218_file_split_on_write_partitioned_dotted_ids for object storage: the number of an object of an
# insert split by size is placed into the path pattern before the partition id is substituted into it, so a
# partition id with a dot in it cannot shift it, and a truncating rewrite of `a.5` deletes only its own numbered
# objects. With the pattern `p_{_partition_id}.tsv`, `p_a.5.1.tsv` would also be the first object of the partition
# `a.5.1`, so an insert split by size is refused for such a pattern before anything is written.

PREFIX="05220_split_on_write_partitioned/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers of a single partition and is bigger than the limit, so every block goes
# into an object of its own, and the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 500"
OBJECTS="s3(s3_conn, filename='${PREFIX}/p_*.tsv', format=TSV, structure='p String, x UInt64')"

echo '--- The number goes after the partition id, not into it'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/p_{_partition_id}_part.tsv', format=TSV, structure='p String, x UInt64') PARTITION BY p
        SELECT ['a.b', 'a.5', 'a.6'][intDiv(number, 400) + 1] AS p, number AS x FROM numbers(1200) SETTINGS ${SETTINGS};
    SELECT DISTINCT _file FROM ${OBJECTS} ORDER BY _file;
    SELECT p, count(), sum(x), uniqExact(_file) FROM ${OBJECTS} GROUP BY p ORDER BY p;
"

echo '--- A truncating rewrite of a.5 deletes only its own numbered objects'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/p_{_partition_id}_part.tsv', format=TSV, structure='p String, x UInt64') PARTITION BY p
        SELECT 'a.5' AS p, number AS x FROM numbers(100) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
    SELECT DISTINCT _file FROM ${OBJECTS} ORDER BY _file;
    SELECT p, count(), sum(x), uniqExact(_file) FROM ${OBJECTS} GROUP BY p ORDER BY p;
"

echo '--- A split is refused when a numbered name of a partition may be the first name of another partition'
for truncate in 0 1
do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/p_{_partition_id}.tsv', format=TSV, structure='p String, x UInt64') PARTITION BY p
            SELECT 'a.5' AS p, number AS x FROM numbers(1000) SETTINGS ${SETTINGS}, s3_truncate_on_insert = ${truncate};
    " 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
done
${CLICKHOUSE_CLIENT} --query "SELECT DISTINCT _file FROM ${OBJECTS} ORDER BY _file"
