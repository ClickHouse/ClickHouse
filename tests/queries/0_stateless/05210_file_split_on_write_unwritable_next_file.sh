#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A file of a split insert becomes a part of the table only after it has been written. If the insert cannot
# create the next file, the table must not keep a name that a `SELECT` would then fail on with
# `FILE_DOESNT_EXIST`: it keeps reading exactly the files that are on disk.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

echo '--- The second file of the insert cannot be created'
# A dangling symlink is not an existing file, so the insert picks this name and then fails to open it.
ln -s "${DIR}/missing_directory/target.tsv" "${DIR}/data.1.tsv"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}" 2>&1 | grep -o -m1 'FILE_DOESNT_EXIST'

echo '--- The table reads the file that was written, and not the one that could not be created'
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x) FROM test"

echo '--- The rest of the data is written after the obstacle is gone'
rm "${DIR}/data.1.tsv"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000, 200) SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), max(x) FROM test"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
ls "${DIR}" | sort

rm -rf "${DIR}"
