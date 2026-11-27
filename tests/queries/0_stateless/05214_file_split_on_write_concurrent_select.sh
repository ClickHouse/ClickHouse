#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A split insert publishes the files it has written one by one, and a truncating split insert deletes the
# files of the previous insert that it does not rewrite. A concurrent `SELECT` must see either the whole
# previous insert or the whole new one: neither a prefix of the files of an insert that is in progress, nor a
# file that the insert deletes before the `SELECT` opens it. The set of the files is taken under the same lock
# that the insert holds for its whole duration.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written, so an
# insert of 1000 numbers is split into 4 files, and an insert of 300 numbers into 2.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000, engine_file_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"

function write_thread()
{
    for _ in $(seq 1 15)
    do
        ${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000, 300) SETTINGS ${SETTINGS}"
        ${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"
    done
}

function read_thread()
{
    while [ ! -f "${DIR}/writer_finished" ]
    do
        # An error of the `SELECT` goes to the output and fails the test.
        result=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test" 2>&1)
        if [ "${result}" != "300" ] && [ "${result}" != "1000" ]
        then
            echo "Unexpected result: ${result}"
        fi
    done
}

write_thread &
WRITER_PID=$!
read_thread &
read_thread &
wait "${WRITER_PID}"
touch "${DIR}/writer_finished"
wait

${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test"
ls "${DIR}" | grep -v writer_finished

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
rm -rf "${DIR}"
