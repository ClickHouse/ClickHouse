#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `JSON` is a format that cannot be appended to. A second partitioned insert into the same partitions finds
# the files of the first one non-empty: with `engine_file_allow_create_multiple_files` it steps aside into the
# first free numbered file of the partition and continues the numbering of the split from there, exactly as
# a non-partitioned insert does; without the setting it is refused.

DIR="${CLICKHOUSE_TMP}/05219_split_on_write_partitioned"
rm -rf "${DIR}"
mkdir -p "${DIR}"

# Every block is exactly 100 numbers - 50 per partition - and every block is bigger than the limit, so every
# block goes into a file of its own, and the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1"
FILES="file('${DIR}/p_*.json', JSON, 'p UInt64, x UInt64')"

echo '--- The first insert'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}.json', JSON, 'p UInt64, x UInt64') PARTITION BY p
        SELECT number % 2 AS p, number AS x FROM numbers(400) SETTINGS ${SETTINGS}, engine_file_allow_create_multiple_files = 1;
"
ls "${DIR}" | LC_ALL=C sort

echo '--- The second insert steps aside from the non-empty first file of every partition into the next numbered one'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}.json', JSON, 'p UInt64, x UInt64') PARTITION BY p
        SELECT number % 2 AS p, number AS x FROM numbers(400, 400) SETTINGS ${SETTINGS}, engine_file_allow_create_multiple_files = 1;
"
ls "${DIR}" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT p, count(), sum(x), uniqExact(_file) FROM ${FILES} GROUP BY p ORDER BY p"

echo '--- Without the setting the insert is refused, and nothing is written'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}.json', JSON, 'p UInt64, x UInt64') PARTITION BY p
        SELECT number % 2 AS p, number AS x FROM numbers(800, 400) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -m1 'CANNOT_APPEND_TO_FILE'
ls "${DIR}" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT p, count(), sum(x), uniqExact(_file) FROM ${FILES} GROUP BY p ORDER BY p"

rm -rf "${DIR}"
