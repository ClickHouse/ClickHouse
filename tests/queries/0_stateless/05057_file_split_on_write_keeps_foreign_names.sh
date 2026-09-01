#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/05057_split_foreign_names"
rm -rf "${DIR}"
mkdir -p "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000, engine_file_allow_create_multiple_files = 1"

echo '--- Someone else owns two of the numbered names'
printf 'foreign 1\n' > "${DIR}/data.1.tsv"
printf 'foreign 3\n' > "${DIR}/data.3.tsv"

echo '--- A large insert skips the taken names'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv');
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
"
ls "${DIR}"

echo '--- A smaller truncating insert does not delete the names it does not own'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv');
    INSERT INTO test SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
"
cat "${DIR}/data.1.tsv"
cat "${DIR}/data.3.tsv"

rm -rf "${DIR}"
