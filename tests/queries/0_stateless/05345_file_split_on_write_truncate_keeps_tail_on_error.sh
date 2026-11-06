#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# If the first file of a `File` table cannot be truncated, `TRUNCATE TABLE` fails before it deletes the numbered
# files written by the inserts split by size, and the table goes on reading all of them.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test"
ls "${DIR}" | sort

# The first file is replaced by a directory, which cannot be truncated.
mv "${DIR}/data.tsv" "${DIR}/saved.tsv"
mkdir "${DIR}/data.tsv"

${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test" 2>&1 | grep -o -m1 "CANNOT_TRUNCATE_FILE"
ls "${DIR}" | sort

rmdir "${DIR}/data.tsv"
mv "${DIR}/saved.tsv" "${DIR}/data.tsv"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM test"

${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test"
ls "${DIR}" | sort

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
rm -rf "${DIR}"
