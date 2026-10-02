#!/usr/bin/env bash
# In a FileLog directory, a file that cannot be opened and a file with lines that do not parse
# (default handle_error_mode) must not stop the table: both are logged and the other rows still arrive.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
target=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_target.jsonl
bad=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_bad.jsonl
sel_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_sel
sel_target=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_sel_target.jsonl
rm -rf "${logs_dir:?}" "${target}" "${bad}" "${sel_dir:?}" "${sel_target}" "${sel_target}.old"
mkdir -p "${logs_dir}" "${sel_dir}"

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
# An aggregate without GROUP BY gives one row per block it receives, so an empty block would show up as 0.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_count (c UInt64) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_count TO dst_count AS SELECT count() AS c FROM file_log"

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

# Direct reads of a single-file table, which has no directory watcher: a file whose symlink target is gone is skipped
# and logged once, a target created again is read from its start, with the old inode even if the table is reloaded
# first and with a new inode not again after a reload, and a record that does not parse fails the query.
printf '{"a":200}\n' > "${sel_target}"
ln -s "${sel_target}" "${sel_dir}/link.jsonl"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_sel (a UInt64) ENGINE = FileLog('${sel_dir}/link.jsonl', 'JSONEachRow')"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_sel"
# The target is removed and created again with the same inode, as a file system that reuses inode numbers does:
# a second link keeps the inode while the name is gone, the content is rewritten through it and linked back.
ln "${sel_target}" "${sel_target}.old"
rm "${sel_target}"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_sel"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_sel"
printf '{"a":201}\n' > "${sel_target}.old"
ln "${sel_target}.old" "${sel_target}"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log_sel"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log_sel"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_sel"
# The target is removed again and created with a new inode, because `.old` still holds the old one.
rm "${sel_target}"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_sel"
printf '{"a":202}\n' > "${sel_target}"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_sel"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log_sel"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log_sel"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_sel"
printf 'not json\n' >> "${sel_target}"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM file_log_sel" 2>&1 \
    | grep -o -m1 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'

${CLICKHOUSE_CLIENT} -q "SELECT count() > 0, countIf(c = 0) FROM dst_count"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT countIf(message LIKE 'Cannot open file %broken.jsonl%') > 0,
           sumIf(toUInt64OrZero(extract(message, 'Skipped ([0-9]+) records')), message LIKE 'Skipped % of file bad.jsonl%'),
           countIf(message LIKE 'Skipped % of file bad.jsonl%') BETWEEN 1 AND 49,
           maxIf(toUInt64OrZero(extract(message, 'Skipped ([0-9]+) records')), message LIKE 'Skipped % of file bad.jsonl%') <= 10
    FROM system.text_log
    WHERE event_date >= yesterday() AND logger_name LIKE concat('StorageFileLog (%', currentDatabase(), '%.file_log)')"
${CLICKHOUSE_CLIENT} -q "
    SELECT countIf(message LIKE 'Cannot open file %link.jsonl%')
    FROM system.text_log
    WHERE event_date >= yesterday() AND logger_name LIKE concat('StorageFileLog (%', currentDatabase(), '%.file_log_sel)')"

${CLICKHOUSE_CLIENT} -q "DROP TABLE mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE mv_count"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst_count"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_sel"
rm -rf "${logs_dir:?}" "${target}" "${sel_dir:?}" "${sel_target}" "${sel_target}.old"
