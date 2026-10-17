#!/usr/bin/env bash
# A direct SELECT from a FileLog table over a directory that fails on a line that does not parse is not the end of
# the file: the next SELECT reads the line again and fails again, although the file has not changed. In this mode
# the status of a file changes only on directory events, so the failed read has to restore it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
mkdir -p "${logs_dir}"
rm -rf "${logs_dir:?}"/*

# The file exists before the table is created and does not change afterwards.
printf '{"a":1}\nnot json\n' > "${logs_dir}/a.jsonl"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (a UInt64) ENGINE = FileLog('${logs_dir}/', 'JSONEachRow')"

for _ in 1 2; do
    ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log" 2>&1 \
        | grep -o -m1 'CANNOT_PARSE_INPUT_ASSERTION_FAILED' || echo 'no error'
done

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
rm -rf "${logs_dir:?}"
