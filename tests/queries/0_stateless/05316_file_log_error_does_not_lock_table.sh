#!/usr/bin/env bash
# A FileLog read that fails (here a line that does not parse, with the default handle_error_mode)
# must not leave the table locked: the next read works and later lines are consumed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
mkdir -p "${logs_dir}"
rm -rf "${logs_dir:?}"/*

printf '{"a":1}\nnot json\n' > "${logs_dir}/a.jsonl"

# A table on a single file re-checks it on every read, so the test does not wait for a directory watcher.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (a UInt64) ENGINE = FileLog('${logs_dir}/a.jsonl', 'JSONEachRow')"

${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log" 2>&1 \
    | grep -o -m1 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT count() FROM file_log"

printf '{"a":3}\n' >> "${logs_dir}/a.jsonl"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log"

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
rm -rf "${logs_dir:?}"
