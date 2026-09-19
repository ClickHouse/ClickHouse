#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A partition id is data, and it may contain dots. The number of a file of an insert split by size is placed into
# the path pattern before the partition id is substituted into it, so the id cannot shift it: the partition `a.b`
# is written into `p_a.b.tsv`, `p_a.b.1.tsv`, ... rather than `p_a.1.b.tsv`, and the partition `a.5` continues as
# `p_a.5.1.tsv` rather than `p_a.6.tsv` - the first file of the partition `a.6`, which a truncating rewrite of
# `a.5` would otherwise take for a stale file of its own and delete.

DIR="${CLICKHOUSE_TMP}/05218_split_on_write_partitioned"
rm -rf "${DIR}"
mkdir -p "${DIR}"

# Every block is exactly 100 numbers of a single partition and is bigger than the limit, so every block goes
# into a file of its own, and the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 500"
FILES="file('${DIR}/p_*.tsv', TSV, 'p String, x UInt64')"

echo '--- The number goes after the partition id, not into it'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}.tsv', TSV, 'p String, x UInt64') PARTITION BY p
        SELECT ['a.b', 'a.5', 'a.6'][intDiv(number, 400) + 1] AS p, number AS x FROM numbers(1200) SETTINGS ${SETTINGS};
"
ls "${DIR}" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT p, count(), sum(x), uniqExact(_file) FROM ${FILES} GROUP BY p ORDER BY p"

echo '--- A smaller truncating rewrite of one partition deletes only the numbered files of that partition'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}.tsv', TSV, 'p String, x UInt64') PARTITION BY p
        SELECT 'a.5' AS p, number AS x FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
"
ls "${DIR}" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT p, count(), sum(x), uniqExact(_file) FROM ${FILES} GROUP BY p ORDER BY p"

rm -rf "${DIR}"
