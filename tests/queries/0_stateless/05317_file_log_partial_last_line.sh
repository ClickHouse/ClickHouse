#!/usr/bin/env bash
# A FileLog file whose last line has no newline yet (still being written) must not fail the read:
# the complete lines are returned, and the last line is returned, from its start, once it is completed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${logs_dir:?}"
mkdir -p "${logs_dir}/dir"

# A single file is re-read on every SELECT.
printf '{"a":1}\n{"a":2' > "${logs_dir}/file.jsonl"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_file (a UInt64) ENGINE = FileLog('${logs_dir}/file.jsonl', 'JSONEachRow')"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a, _offset FROM file_log_file"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT count() FROM file_log_file"
printf '}\n{"a":3}\n' >> "${logs_dir}/file.jsonl"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a, _offset FROM file_log_file"

# In a directory the completed lines are picked up through the directory watcher. One stream reads both files,
# so an incomplete line in one file must not stop the other.
printf '{"a":10}\n{"a":20' > "${logs_dir}/dir/a.jsonl"
printf '{"a":30}\n{"a":40' > "${logs_dir}/dir/b.jsonl"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_dir (a UInt64) ENGINE = FileLog('${logs_dir}/dir/', 'JSONEachRow') SETTINGS max_threads = 1"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_dir ORDER BY a"
# The directory watch is installed asynchronously after CREATE: wait until it reports a file created now.
for i in {1..300}; do
    printf '{"a":0}\n' > "${logs_dir}/dir/ready_${i}.jsonl"
    [ "$(${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT count() FROM file_log_dir")" -gt 0 ] && break
    sleep 0.1
done
printf '}\n' >> "${logs_dir}/dir/a.jsonl"
printf '}\n' >> "${logs_dir}/dir/b.jsonl"
res=""
for _ in {1..300}; do
    res+=$(${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_dir WHERE a != 0")$'\n'
    [ "$(grep -c . <<< "${res}")" -ge 2 ] && break
    sleep 0.1
done
grep . <<< "${res}" | sort -n

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_file"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_dir"
rm -rf "${logs_dir:?}"
