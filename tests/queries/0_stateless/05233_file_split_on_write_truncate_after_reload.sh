#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `File` table keeps no metadata about the files it has written: `DETACH` / `ATTACH` or a server restart
# rebuilds its list of the paths from the single configured name, so the numbered files of an earlier insert
# split by size are not attributable to the table anymore.
#
# `TRUNCATE TABLE` therefore leaves such a forgotten tail where it is - deleting a file the table does not own
# would be destructive - and only warns about it in the server log. A truncating insert split by size does
# reclaim the tail: it overwrites the whole numbered sequence of the path anyway, so it removes the leftovers
# by number instead of failing on the names that are taken.

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

echo '--- After a reload the table reads only the base file'
${CLICKHOUSE_CLIENT} --query "DETACH TABLE test"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE test"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test"

echo '--- The truncate empties the base file and leaves the forgotten tail in place, with a warning in the log'
${CLICKHOUSE_CLIENT} --send_logs_level=warning --query "TRUNCATE TABLE test" 2>&1 \
    | grep -c -F "has left the file ${DIR}/data.1.tsv in place"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test"
ls "${DIR}" | sort
wc -c < "${DIR}/data.tsv"

echo '--- A truncating split insert reclaims the forgotten tail by number instead of failing on it'
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(300) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM file('${DIR}/data*.tsv', TSV, 'x UInt64')"
ls "${DIR}" | sort

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
rm -rf "${DIR}"
