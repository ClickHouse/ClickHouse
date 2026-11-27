#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The same as 05394_file_split_on_write_partitioned_ambiguous_names for object storage: with the path pattern
# `p_{_partition_id}.tsv`, the first numbered object of the partition `a` is `p_a.1.tsv`, which is also the first
# object of the partition `a.1`, so an insert split by size is refused, with or without truncation, before anything
# is written or deleted. The layouts where the keys cannot be the same are still split and rewritten.

PREFIX="05395_split_on_write_partitioned/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers of a single partition and is bigger than the limit, so every block goes
# into an object of its own, and the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 500"

echo '--- The partition `a.1` is not deleted as a numbered object of the partition `a`'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/ambiguous/p_{_partition_id}.tsv', format=TSV, structure='p String, x UInt64') PARTITION BY p SELECT 'a.1', 1;
"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/ambiguous/p_{_partition_id}.tsv', format=TSV, structure='p String, x UInt64') PARTITION BY p
        SELECT 'a', 2 SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} --query "
    SELECT _file, p, x FROM s3(s3_conn, filename='${PREFIX}/ambiguous/p_*.tsv', format=TSV, structure='p String, x UInt64') ORDER BY _file;
"

echo '--- The partition `a` is not split into `p_a.1.tsv` either, with or without s3_create_new_file_on_insert'
for create_new_file_on_insert in 0 1
do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/ambiguous/p_{_partition_id}.tsv', format=TSV, structure='p String, x UInt64') PARTITION BY p
            SELECT 'a', number FROM numbers(400) SETTINGS ${SETTINGS}, s3_create_new_file_on_insert = ${create_new_file_on_insert};
    " 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
done
${CLICKHOUSE_CLIENT} --query "
    SELECT _file, p, x FROM s3(s3_conn, filename='${PREFIX}/ambiguous/p_*.tsv', format=TSV, structure='p String, x UInt64') ORDER BY _file;
"

# Rewrites the partition `a` written into 4 objects with 100 rows, next to the partition `a.1`.
function rewrite()
{
    local pattern="$1"
    local glob="$2"
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/${pattern}', format=TSV, structure='p String, x UInt64') PARTITION BY p SELECT 'a.1', 1;
        INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/${pattern}', format=TSV, structure='p String, x UInt64') PARTITION BY p
            SELECT 'a', number FROM numbers(400) SETTINGS ${SETTINGS};
        INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/${pattern}', format=TSV, structure='p String, x UInt64') PARTITION BY p
            SELECT 'a', number FROM numbers(100) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
        SELECT p, count(), sum(x), uniqExact(_file) FROM s3(s3_conn, filename='${PREFIX}/${glob}', format=TSV, structure='p String, x UInt64') GROUP BY p ORDER BY p;
        SELECT DISTINCT substring(_path, position(_path, '${PREFIX}/') + length('${PREFIX}/')) AS path FROM s3(s3_conn, filename='${PREFIX}/${glob}', format=TSV, structure='p String, x UInt64') ORDER BY path;
    "
}

echo '--- A character that is not a digit between the partition id and the extension'
rewrite 'suffix/p_{_partition_id}_part.tsv' 'suffix/p_*.tsv'

echo '--- The partition id in a directory'
rewrite 'directory/{_partition_id}/data.tsv' 'directory/*/data*.tsv'

echo '--- A partition key that cannot have a dot in it'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/integer/p_{_partition_id}.tsv', format=TSV, structure='p UInt64, x UInt64') PARTITION BY p
        SELECT 1, number FROM numbers(400) SETTINGS ${SETTINGS};
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/integer/p_{_partition_id}.tsv', format=TSV, structure='p UInt64, x UInt64') PARTITION BY p
        SELECT 1, number FROM numbers(100) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
    SELECT DISTINCT _file FROM s3(s3_conn, filename='${PREFIX}/integer/p_*.tsv', format=TSV, structure='p UInt64, x UInt64') ORDER BY _file;
    SELECT p, count(), sum(x) FROM s3(s3_conn, filename='${PREFIX}/integer/p_*.tsv', format=TSV, structure='p UInt64, x UInt64') GROUP BY p ORDER BY p;
"
