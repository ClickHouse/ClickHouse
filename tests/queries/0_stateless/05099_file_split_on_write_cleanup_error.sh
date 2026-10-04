#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/05099_split_cleanup_error"
rm -rf "${DIR}"
mkdir -p "${DIR}"

SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000, engine_file_truncate_on_insert = 1"

echo '--- Split into numbered files'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv');
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
"
ls "${DIR}"

echo '--- The first numbered name of the sequence is replaced by a non-empty directory, which cannot be removed'
rm "${DIR}/data.1.tsv"
mkdir "${DIR}/data.1.tsv"
touch "${DIR}/data.1.tsv/inner"

echo '--- A truncating insert fails instead of leaving the stale data in place'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv');
    INSERT INTO test SELECT number FROM numbers(100) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -m1 'CANNOT_UNLINK'

echo '--- The base file was not touched, and the whole tail of the sequence is still there'
${CLICKHOUSE_LOCAL} --query "SELECT count(), min(x), max(x) FROM file('${DIR}/data.tsv', TSV, 'x UInt64')"
ls "${DIR}"

rm -rf "${DIR}"
