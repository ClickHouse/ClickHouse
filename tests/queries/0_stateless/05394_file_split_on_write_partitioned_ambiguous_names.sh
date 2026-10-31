#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With the path pattern `p_{_partition_id}.tsv`, the first numbered file of the partition `a` is `p_a.1.tsv`,
# which is also the first file of the partition `a.1`. A truncating insert split by size deletes the numbered files
# of the partition it rewrites, and it cannot tell them from the files of other partitions. An insert split by size
# without truncation writes `p_a.1.tsv`, which a truncating insert into the partition `a.1` would overwrite later.
# So an insert split by size is refused for such a layout before anything is written or deleted. The layouts where
# the names cannot be the same are still split and rewritten.

DIR="${CLICKHOUSE_TMP}/05394_split_on_write_partitioned"
rm -rf "${DIR}"
mkdir -p "${DIR}"

# Every block is exactly 100 numbers of a single partition and is bigger than the limit, so every block goes
# into a file of its own, and the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 500"

echo '--- The partition `a.1` is not deleted as a numbered file of the partition `a`'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/ambiguous/p_{_partition_id}.tsv', TSV, 'p String, x UInt64') PARTITION BY p SELECT 'a.1', 1;
"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/ambiguous/p_{_partition_id}.tsv', TSV, 'p String, x UInt64') PARTITION BY p
        SELECT 'a', 2 SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
ls "${DIR}/ambiguous" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT p, x FROM file('${DIR}/ambiguous/p_*.tsv', TSV, 'p String, x UInt64') ORDER BY p"

echo '--- The partition `a` is not split into `p_a.1.tsv` either, with or without engine_file_allow_create_multiple_files'
for allow_create_multiple_files in 0 1
do
    ${CLICKHOUSE_LOCAL} --query "
        INSERT INTO FUNCTION file('${DIR}/ambiguous/p_{_partition_id}.tsv', TSV, 'p String, x UInt64') PARTITION BY p
            SELECT 'a', number FROM numbers(400) SETTINGS ${SETTINGS}, engine_file_allow_create_multiple_files = ${allow_create_multiple_files};
    " 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
done
ls "${DIR}/ambiguous" | LC_ALL=C sort

# Rewrites the partition `a` written into 4 files with 100 rows, next to the partition `a.1`.
function rewrite()
{
    local pattern="$1"
    local glob="$2"
    ${CLICKHOUSE_LOCAL} --query "
        INSERT INTO FUNCTION file('${DIR}/${pattern}', TSV, 'p String, x UInt64') PARTITION BY p SELECT 'a.1', 1;
        INSERT INTO FUNCTION file('${DIR}/${pattern}', TSV, 'p String, x UInt64') PARTITION BY p
            SELECT 'a', number FROM numbers(400) SETTINGS ${SETTINGS};
        INSERT INTO FUNCTION file('${DIR}/${pattern}', TSV, 'p String, x UInt64') PARTITION BY p
            SELECT 'a', number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
        SELECT p, count(), sum(x), uniqExact(_file) FROM file('${DIR}/${glob}', TSV, 'p String, x UInt64') GROUP BY p ORDER BY p;
    "
}

echo '--- A character that is not a digit between the partition id and the extension'
rewrite 'suffix/p_{_partition_id}_part.tsv' 'suffix/p_*.tsv'
ls "${DIR}/suffix" | LC_ALL=C sort

echo '--- The partition id in a directory'
rewrite 'directory/{_partition_id}/data.tsv' 'directory/*/data*.tsv'
find "${DIR}/directory" -type f | sed "s|${DIR}/directory/||" | LC_ALL=C sort

echo '--- A partition key that cannot have a dot in it'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/integer/p_{_partition_id}.tsv', TSV, 'p UInt64, x UInt64') PARTITION BY p
        SELECT 1, number FROM numbers(400) SETTINGS ${SETTINGS};
    INSERT INTO FUNCTION file('${DIR}/integer/p_{_partition_id}.tsv', TSV, 'p UInt64, x UInt64') PARTITION BY p
        SELECT 1, number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
    SELECT p, count(), sum(x), uniqExact(_file) FROM file('${DIR}/integer/p_*.tsv', TSV, 'p UInt64, x UInt64') GROUP BY p ORDER BY p;
"
ls "${DIR}/integer" | LC_ALL=C sort

rm -rf "${DIR}"
