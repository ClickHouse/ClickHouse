#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `TRUNCATE TABLE` empties a `File` table that has been written by an insert split by size: the numbered
# files of the previous inserts are deleted and forgotten, not just truncated in place, so that the next
# split insert starts the numbering from a clean state instead of finding its names taken by the leftovers.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

echo '--- A split insert'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test"
ls "${DIR}" | sort

echo '--- The table is truncated: the numbered files are gone, the first one is empty'
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test"
ls "${DIR}" | sort
wc -c < "${DIR}/data.tsv"

echo '--- The next split insert starts the numbering over'
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000, 500) SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test"
ls "${DIR}" | sort

echo '--- Truncated again, and read both from the table and by a glob over the directory'
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(300) SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM file('${DIR}/data*.tsv', TSV, 'x UInt64')"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
ls "${DIR}" | sort

rm -rf "${DIR}"
