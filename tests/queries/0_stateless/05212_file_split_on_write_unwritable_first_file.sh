#!/usr/bin/env bash
# Tags: no-fasttest
# Fast test builds without Parquet, which this test uses as a format that cannot be appended to.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `engine_file_allow_create_multiple_files`, an insert into a non-empty file of a format that cannot be
# appended to steps aside into a new numbered file. That file becomes a part of the table only after it has been
# written: if the insert cannot create it, the table must not keep a name that a `SELECT` would then fail on
# with `FILE_DOESNT_EXIST`. This holds both for a plain insert and for an insert split by size, where the
# same rule already applies to the files after the first one.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

SETTINGS="max_threads = 1, max_insert_threads = 1, engine_file_allow_create_multiple_files = 1"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x UInt64) ENGINE = File(Parquet, '${DIR}/data.parquet')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(300) SETTINGS ${SETTINGS}"

echo '--- The first file of a split insert cannot be created'
# A dangling symlink is not an existing file, so the insert picks this name and then fails to open it.
ln -s "${DIR}/missing_directory/target.parquet" "${DIR}/data.1.parquet"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(300, 100) SETTINGS ${SETTINGS}, engine_file_split_on_write_by_size_bytes = 1000000" 2>&1 | grep -o -m1 'FILE_DOESNT_EXIST'

echo '--- The table reads the file that was written, and not the one that could not be created'
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test"

echo '--- The same for a plain insert that steps aside into a new file'
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(300, 100) SETTINGS ${SETTINGS}" 2>&1 | grep -o -m1 'FILE_DOESNT_EXIST'
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test"

echo '--- The rest of the data is written after the obstacle is gone'
rm "${DIR}/data.1.parquet"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(300, 100) SETTINGS ${SETTINGS}, engine_file_split_on_write_by_size_bytes = 1000000"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
ls "${DIR}" | sort

rm -rf "${DIR}"
