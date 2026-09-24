#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A truncating insert that is itself split by size rolls over into the numbered names - and with
# `engine_file_allow_create_multiple_files` the numbered names are a shared namespace, so the rewrite
# has to step over the names it does not own instead of overwriting them.

DIR="${CLICKHOUSE_TMP}/05235_split_truncating_rewrite_foreign_names"
rm -rf "${DIR}"
mkdir -p "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000, engine_file_allow_create_multiple_files = 1"

echo '--- Someone else owns two of the numbered names'
printf 'foreign 1\n' > "${DIR}/data.1.tsv"
printf 'foreign 3\n' > "${DIR}/data.3.tsv"

${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv');

    SELECT '--- A large insert skips the taken names';
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT count(), min(x), max(x) FROM test;

    SELECT '--- A truncating rewrite that needs the numbered names as well';
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
    SELECT count(), min(x), max(x) FROM test;
"

echo '--- The names the rewrite does not own are intact'
cat "${DIR}/data.1.tsv"
cat "${DIR}/data.3.tsv"

echo '--- The files of the previous insert are deleted, and the rewrite has stepped over the foreign names'
ls "${DIR}"

rm -rf "${DIR}"
