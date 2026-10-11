#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/05045_split_reinsert"
rm -rf "${DIR}"
mkdir -p "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

echo '--- A truncating insert into the same table starts the numbering over and overwrites the split files'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/truncate/data.tsv');
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    INSERT INTO test SELECT number FROM numbers(500) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
    SELECT _file, count(), min(x), max(x) FROM test GROUP BY _file ORDER BY _file;
    SELECT count(), sum(x) FROM test;
"

echo '--- A second insert continues after the taken names with engine_file_allow_create_multiple_files'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/multiple/data.tsv');
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    INSERT INTO test SELECT number FROM numbers(500) SETTINGS ${SETTINGS}, engine_file_allow_create_multiple_files = 1;
    SELECT _file, count() FROM test GROUP BY _file ORDER BY _file;
    SELECT count(), sum(x) FROM test;
"

echo '--- An exception if the names of the split files are taken by the previous insert'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/exists/data.tsv');
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -m1 'FILE_ALREADY_EXISTS'

rm -rf "${DIR}"
