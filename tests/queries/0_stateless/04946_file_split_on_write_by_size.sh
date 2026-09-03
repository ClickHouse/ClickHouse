#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on the Parquet format

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/04946_split_on_write"
rm -rf "${DIR}"
mkdir -p "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

echo '--- Split into numbered files'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/split/data.tsv', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
"
ls "${DIR}/split" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "
    SELECT _file, count(), min(x), max(x) FROM file('${DIR}/split/data*.tsv', TSV, 'x UInt64') GROUP BY _file ORDER BY _file;
    SELECT count(), sum(x) FROM file('${DIR}/split/data*.tsv', TSV, 'x UInt64');
"

echo '--- The numbering continues from the number in the name of the first file'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/offset/data.5.tsv', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
"
ls "${DIR}/offset" | LC_ALL=C sort

echo '--- The number can be in the name of the first file'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/numbered/data.0.tsv', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
"
ls "${DIR}/numbered" | LC_ALL=C sort

echo '--- A non-numeric part of the name is not a sequence number'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/dotted/data.v2.tsv', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
"
ls "${DIR}/dotted" | LC_ALL=C sort

echo '--- A new file on each insert continues the numbering as well'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/multiple/data.5.Parquet', Parquet, 'x UInt64') SELECT 1;
    INSERT INTO FUNCTION file('${DIR}/multiple/data.5.Parquet', Parquet, 'x UInt64') SELECT 2 SETTINGS engine_file_allow_create_multiple_files = 1;
"
ls "${DIR}/multiple" | LC_ALL=C sort

echo '--- Without splitting everything goes into a single file'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/single/data.tsv', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, engine_file_split_on_write_by_size_bytes = 0;
"
ls "${DIR}/single" | LC_ALL=C sort

echo '--- The files are truncated on the second insert with engine_file_truncate_on_insert'
for _ in 1 2; do
    ${CLICKHOUSE_LOCAL} --query "
        INSERT INTO FUNCTION file('${DIR}/truncate/data.tsv', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
    "
done
ls "${DIR}/truncate" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(x) FROM file('${DIR}/truncate/data*.tsv', TSV, 'x UInt64')"

echo '--- All the split files are visible for the File engine table'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/engine/data.tsv');
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT count(), sum(x), uniqExact(_file) FROM test;
"

echo '--- An exception if the name is already taken'
mkdir -p "${DIR}/exists"
touch "${DIR}/exists/data.1.tsv"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/exists/data.tsv', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -m1 'FILE_ALREADY_EXISTS'

echo '--- The taken names are skipped with engine_file_allow_create_multiple_files'
mkdir -p "${DIR}/skip"
touch "${DIR}/skip/data.1.tsv"
touch "${DIR}/skip/data.3.tsv"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/skip/data.tsv', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, engine_file_allow_create_multiple_files = 1;
"
ls "${DIR}/skip" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(x) FROM file('${DIR}/skip/data*.tsv', TSV, 'x UInt64')"

echo '--- Every file is complete on its own, for a format with a prefix and a suffix'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/parquet/data.Parquet', Parquet, 'x UInt64') SELECT number FROM numbers(1000)
        SETTINGS ${SETTINGS}, engine_file_split_on_write_by_size_bytes = 1, output_format_parquet_row_group_size = 100;
    SELECT count(), sum(x), uniqExact(_file) FROM file('${DIR}/parquet/data*.Parquet', Parquet, 'x UInt64');
"

echo '--- The size of the compressed data is taken into account'
# The compressed data reaches the file only when the compression buffer is full,
# so the files are much larger than the requested size, and there has to be enough data to fill the buffer.
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/compressed/data.tsv.gz', TSV, 'x UInt64') SELECT number FROM numbers(1000000)
        SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 100000, min_insert_block_size_rows = 100000,
            min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 4096;
    SELECT count(), sum(x), uniqExact(_file) > 1 FROM file('${DIR}/compressed/data*.tsv.gz', TSV, 'x UInt64');
"

echo '--- Every partition is split on its own'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/partitioned/p_{_partition_id}.tsv', TSV, 'p UInt64, x UInt64') PARTITION BY p
        SELECT number % 2 AS p, number AS x FROM numbers(1000) SETTINGS ${SETTINGS};
"
ls "${DIR}/partitioned" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT p, count(), sum(x) FROM file('${DIR}/partitioned/p_*.tsv', TSV, 'p UInt64, x UInt64') GROUP BY p ORDER BY p"

echo '--- It is not possible to split the data written into a file descriptor'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, stdout);
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'

rm -rf "${DIR}"
