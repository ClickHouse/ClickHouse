#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/05136_split_truncate_without_split"
rm -rf "${DIR}"
mkdir -p "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0"

echo '--- A large split insert, then a truncating rewrite with the splitting turned off'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv');
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, engine_file_split_on_write_by_size_bytes = 1000;
    INSERT INTO test SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_split_on_write_by_size_bytes = 0, engine_file_truncate_on_insert = 1;
    SELECT 'engine table', count(), min(x), max(x) FROM test;
    SELECT 'wildcard', count(), min(x), max(x) FROM file('${DIR}/data*.tsv', TSV, 'x UInt64');
"
ls "${DIR}"

echo '--- The stale rows are not visible for a new reader either'
${CLICKHOUSE_LOCAL} --query "
    SELECT count(), min(x), max(x) FROM file('${DIR}/data*.tsv', TSV, 'x UInt64');
"

rm -rf "${DIR}"
