#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An insert with `engine_file_allow_create_multiple_files` steps over the numbered names it does not own.
# A later truncating insert with that setting turned back off must not delete them: a table knows the files
# it has written itself, and deletes exactly them.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

echo '--- Someone else owns two of the numbered names'
printf 'foreign 1\n' > "${DIR}/data.1.tsv"
printf 'foreign 3\n' > "${DIR}/data.3.tsv"

echo '--- A large insert skips the taken names'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv');
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, engine_file_allow_create_multiple_files = 1;
    SELECT count(), min(x), max(x) FROM test;
"
ls "${DIR}"

echo '--- A smaller truncating insert with engine_file_allow_create_multiple_files back at 0'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1, engine_file_allow_create_multiple_files = 0;
    SELECT count(), min(x), max(x) FROM test;
    DROP TABLE test;
"

echo '--- The files of the previous insert are deleted, the names it did not own are intact'
ls "${DIR}"
cat "${DIR}/data.1.tsv"
cat "${DIR}/data.3.tsv"

rm -rf "${DIR}"
