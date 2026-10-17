#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A truncating insert deletes the numbered files of the previous insert. If a file cannot be deleted,
# the insert fails - and the table has to keep the whole list of its files, so that the ones that are
# still on disk stay visible for reading in the same server process.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

echo '--- A large insert produces multiple numbered files'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv');
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT count(), min(x), max(x) FROM test;
"
ls "${DIR}"

echo '--- The first numbered file is replaced by a non-empty directory, which cannot be removed'
mv "${DIR}/data.1.tsv" "${DIR}/saved.tsv"
mkdir "${DIR}/data.1.tsv"
touch "${DIR}/data.1.tsv/inner"

echo '--- A truncating insert fails instead of leaving the stale data in place'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
" 2>&1 | grep -o -m1 'CANNOT_UNLINK'

echo '--- The table still reads the whole set of its files'
rm -rf "${DIR}/data.1.tsv"
mv "${DIR}/saved.tsv" "${DIR}/data.1.tsv"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), min(x), max(x) FROM test;
    DROP TABLE test;
"

rm -rf "${DIR}"
