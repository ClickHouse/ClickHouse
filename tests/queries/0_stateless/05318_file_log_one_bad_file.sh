#!/usr/bin/env bash
# In a FileLog directory, a file that cannot be opened and a file with lines that do not parse
# (default handle_error_mode) must not stop the table: both are logged and the other rows still arrive.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
target=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_target.jsonl
bad=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_bad.jsonl
rm -rf "${logs_dir:?}" "${target}" "${bad}"
mkdir -p "${logs_dir}"

function wait_for_rows()
{
    for _ in {1..240}; do
        [ "$(${CLICKHOUSE_CLIENT} -q 'SELECT count() FROM dst')" -ge "$1" ] && return
        sleep 0.5
    done
}

printf '{"a":1}\n{"a":2}\n' > "${logs_dir}/good.jsonl"
printf '{"a":100}\n' > "${target}"
ln -s "${target}" "${logs_dir}/broken.jsonl"

# One stream and one record per poll: a run of broken lines then makes polls that return no rows.
# A short backoff bounds how soon a file that could not be opened is retried.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (a UInt64) ENGINE = FileLog('${logs_dir}/', 'JSONEachRow')
    SETTINGS max_threads = 1, poll_max_batch_size = 1,
             poll_directory_watch_events_backoff_init = 500, poll_directory_watch_events_backoff_max = 1000"
# The table tracks broken.jsonl; once its target is gone, the file cannot be opened.
rm "${target}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst (file String, a UInt64) ENGINE = MergeTree ORDER BY (file, a)"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv TO dst AS SELECT _filename AS file, a FROM file_log"

wait_for_rows 2
${CLICKHOUSE_CLIENT} -q "SELECT file, a FROM dst ORDER BY file, a"

# The file becomes readable again without any event in the directory: it is retried and read.
printf '{"a":100}\n' > "${target}"
wait_for_rows 3
${CLICKHOUSE_CLIENT} -q "SELECT file, a FROM dst WHERE file = 'broken.jsonl'"

# A file with lines that do not parse is moved into the directory, as logrotate does with a compressed log.
# No later event follows, so its last line arrives only if reading continues past the broken lines.
{ echo '{"a":10}'; for _ in {1..50}; do echo 'not json'; done; echo '{"a":20}'; } > "${bad}"
mv "${bad}" "${logs_dir}/bad.jsonl"
wait_for_rows 5
${CLICKHOUSE_CLIENT} -q "SELECT file, a FROM dst WHERE file = 'bad.jsonl' ORDER BY a"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT countIf(message LIKE 'Cannot open file %broken.jsonl%') > 0,
           countIf(message LIKE 'Skipped % of file bad.jsonl%') > 0
    FROM system.text_log
    WHERE event_date >= yesterday() AND logger_name LIKE concat('StorageFileLog (%', currentDatabase(), '%.file_log)')"

${CLICKHOUSE_CLIENT} -q "DROP TABLE mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
rm -rf "${logs_dir:?}" "${target}"
