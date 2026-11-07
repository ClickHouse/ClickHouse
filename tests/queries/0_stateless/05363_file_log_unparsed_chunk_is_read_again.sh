#!/usr/bin/env bash
# A direct SELECT from FileLog that fails on a line that does not parse stores the read positions of the chunks
# returned before the failure only: those lines are not read again, the lines of the failed chunk are,
# also after the table is attached again.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
mkdir -p "${logs_dir}"
rm -rf "${logs_dir:?}"/*

printf '{"a":1}\n{"a":2}\nnot json\n{"a":4}\n' > "${logs_dir}/a.jsonl"

# `max_block_size = 1`: every line is a chunk of its own.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (a UInt64) ENGINE = FileLog('${logs_dir}/a.jsonl', 'JSONEachRow') SETTINGS max_block_size = 1"

${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log" 2>&1 \
    | grep -o -m1 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'

${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log"

# The first two lines keep their length, so the stored position is the start of the third line.
printf '{"a":1}\n{"a":2}\n{"a":3}\n{"a":4}\n' > "${logs_dir}/a.jsonl"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log"

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
rm -rf "${logs_dir:?}"
