#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# After a reload, `TRUNCATE TABLE` of a `File` table leaves the numbered files of an earlier insert split by size
# in place and warns about them. The numbered files do not have to start from 1 or to be contiguous: an insert with
# `engine_file_allow_create_multiple_files` steps over the names taken by someone else. The warning names every
# leftover numbered file, not only the first one, and does not name the files that only look similar.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE test"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE test"

# A gap at the start of the numbered sequence, and the files whose names only look like numbered ones.
rm "${DIR}/data.1.tsv"
touch "${DIR}/data.01.tsv" "${DIR}/data.x.tsv" "${DIR}/data.1.csv" "${DIR}/other.1.tsv"

${CLICKHOUSE_CLIENT} --send_logs_level=warning --query "TRUNCATE TABLE test" 2>&1 \
    | grep -o -E "has left the file [^ ]+ in place" | sed "s#${DIR}/##" | sort
${CLICKHOUSE_CLIENT} --send_logs_level=warning --query "TRUNCATE TABLE test" 2>&1 | grep -c -F "has left the file"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test"
ls "${DIR}" | sort

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
rm -rf "${DIR}"
