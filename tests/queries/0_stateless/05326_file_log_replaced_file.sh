#!/usr/bin/env bash
# A FileLog table reads a file that replaced another one under the same name (moved over it, created again after the
# old file was renamed by a rotation, a symlink moved over it, or the target of a symlink replaced) once and from its
# start, and the rotated file from its own offset, also when the replacement happens before the table learns about it.
# A single-file table reads a replaced file from its start, or from an offset set after the replacement, also after the
# table is attached again. A reset of a name that leads to a file read under another name fails.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

base=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${base:?}"
# `outside` is not watched and is on the same file system, so `mv` from there is an atomic rename with a new inode.
mkdir -p "${base}/replace" "${base}/rotate" "${base}/single" "${base}/outside" "${base}/link" "${base}/target"

settings="poll_directory_watch_events_backoff_init = 100, poll_directory_watch_events_backoff_max = 500"

# Waits until table $1 applied the directory event of a new empty file in $2, i.e. it watches $2, without reading data.
function wait_watched()
{
    for i in {1..120}; do
        : > "$2/w$i.csv"
        sleep 0.5
        ${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG $1 FILE 'w$i.csv'" 2>/dev/null && return
    done
    echo timeout
}

function wait_count()
{
    for _ in {1..600}; do
        [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $1")" -ge "$2" ] && return
        sleep 0.2
    done
}

# Prints the values of table $1 once $2 is among them.
function rows_after()
{
    for _ in {1..600}; do
        [ "$(${CLICKHOUSE_CLIENT} -q "SELECT countIf(a = $2) FROM $1")" -ge 1 ] && break
        sleep 0.2
    done
    ${CLICKHOUSE_CLIENT} -q "SELECT groupArray(a) FROM (SELECT a FROM $1 ORDER BY a)"
}

# The view is created after the replacement, so its first read happens before the table applies the events about it.
# The value appended at the end is read in a later round than the new file.
echo '-- moved over'
seq 100 109 > "${base}/replace/a.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_replace (a UInt64) ENGINE = FileLog('${base}/replace/', 'CSV') SETTINGS ${settings}"
wait_watched file_log_replace "${base}/replace"
seq 200 209 > "${base}/outside/a.csv"
mv "${base}/outside/a.csv" "${base}/replace/a.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_replace (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_replace TO dst_replace AS SELECT a FROM file_log_replace"
wait_count dst_replace 10
echo 999 >> "${base}/replace/a.csv"
rows_after dst_replace 999

echo '-- rotated'
seq 100 109 > "${base}/rotate/a.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_rotate (a UInt64) ENGINE = FileLog('${base}/rotate/', 'CSV') SETTINGS ${settings}"
wait_watched file_log_rotate "${base}/rotate"
# The rotated file has a saved offset (its first five values are skipped), larger than the size of the new file.
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log_rotate FILE 'a.csv' OFFSET 20"
# As `logrotate` does in its default `create` mode.
mv "${base}/rotate/a.csv" "${base}/rotate/a.csv.1"
seq 200 202 > "${base}/rotate/a.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_rotate (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_rotate TO dst_rotate AS SELECT a FROM file_log_rotate"
wait_count dst_rotate 8
echo 999 >> "${base}/rotate/a.csv"
rows_after dst_rotate 999

echo '-- symlink moved over'
seq 100 109 > "${base}/link/a.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_link (a UInt64) ENGINE = FileLog('${base}/link/', 'CSV') SETTINGS ${settings}, max_threads = 1"
wait_watched file_log_link "${base}/link"
seq 300 309 > "${base}/outside/t.csv"
ln -s "${base}/outside/t.csv" "${base}/outside/l.csv"
mv "${base}/outside/l.csv" "${base}/link/a.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_link (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_link TO dst_link AS SELECT a FROM file_log_link"
wait_count dst_link 10
# Appends to the target are not seen, so the later value is in a new file; one stream reads the files in order.
echo 999 > "${base}/link/z.csv"
rows_after dst_link 999

echo '-- symlink target replaced'
seq 300 309 > "${base}/outside/u.csv"
ln -s "${base}/outside/u.csv" "${base}/target/a.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_target (a UInt64) ENGINE = FileLog('${base}/target/', 'CSV') SETTINGS ${settings}, max_threads = 1"
wait_watched file_log_target "${base}/target"
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log_target FILE 'a.csv' OFFSET 20"
# The symlink stays and nothing changes in the watched directory. The old target is kept, so the new one has another inode.
mv "${base}/outside/u.csv" "${base}/outside/u.csv.1"
seq 400 409 > "${base}/outside/u.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_target (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_target TO dst_target AS SELECT a FROM file_log_target"
wait_count dst_target 10
echo 999 > "${base}/target/z.csv"
rows_after dst_target 999
# Now the symlink leads to a file that is read as z.csv, so a reset of a.csv cannot take effect.
ln "${base}/target/z.csv" "${base}/outside/u.new"
mv "${base}/outside/u.new" "${base}/outside/u.csv"
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log_target FILE 'a.csv'" 2>&1 | grep -o -m1 BAD_ARGUMENTS

echo '-- single file'
printf '1\n2\n3\n' > "${base}/single/s.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_single (a UInt64) ENGINE = FileLog('${base}/single/s.csv', 'CSV')"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT groupArray(a) FROM (SELECT a FROM file_log_single ORDER BY a)"
printf '10\n20\n30\n40\n' > "${base}/outside/s.csv"
mv "${base}/outside/s.csv" "${base}/single/s.csv"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT groupArray(a) FROM (SELECT a FROM file_log_single ORDER BY a)"
printf '50\n60\n70\n80\n' > "${base}/outside/s.csv"
mv "${base}/outside/s.csv" "${base}/single/s.csv"
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log_single FILE 's.csv' OFFSET 6"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT groupArray(a) FROM (SELECT a FROM file_log_single ORDER BY a)"
# The offset set after a replacement is also kept when the table is attached again before reading.
printf '90\n100\n' > "${base}/outside/s.csv"
mv "${base}/outside/s.csv" "${base}/single/s.csv"
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log_single FILE 's.csv' OFFSET 3"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log_single"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log_single"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT groupArray(a) FROM (SELECT a FROM file_log_single ORDER BY a)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE mv_replace"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst_replace"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_replace"
${CLICKHOUSE_CLIENT} -q "DROP TABLE mv_rotate"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst_rotate"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_rotate"
${CLICKHOUSE_CLIENT} -q "DROP TABLE mv_link"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst_link"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_link"
${CLICKHOUSE_CLIENT} -q "DROP TABLE mv_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_single"
rm -rf "${base:?}"
