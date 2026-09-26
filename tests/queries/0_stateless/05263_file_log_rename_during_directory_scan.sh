#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the test pauses a server-wide fail point that every FileLog directory watcher on macOS
# passes, and waits for it.

# A file renamed while a FileLog table rescans its directory keeps its read offset, so its rows are not
# read twice, and a file moved out of the directory does not stop later changes from being picked up.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if [ "$(${CLICKHOUSE_CLIENT} -q "SELECT value = 'Darwin' FROM system.build_options WHERE name = 'SYSTEM'")" != 1 ]; then
    echo "@@SKIP@@: only the macOS FileLog watcher rescans the directory"
    exit 0
fi

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
moved_dir=${logs_dir}_moved
rm -rf "${logs_dir}" "${moved_dir}"
mkdir -p "${logs_dir}" "${moved_dir}"
printf '1\n2\n3\n' > "${logs_dir}/a.txt"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT file_log_directory_watcher_pause_after_listing"' EXIT

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE file_log (k UInt32) ENGINE = FileLog('${logs_dir}/', 'CSV')
    SETTINGS poll_directory_watch_events_backoff_init = 100, poll_directory_watch_events_backoff_max = 100"

function select_all()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT k FROM file_log ORDER BY k SETTINGS stream_like_engine_allow_direct_select = 1"
}

# Prints the first non-empty result of `select_all` within 10 seconds.
function wait_rows()
{
    local out=""
    local deadline=$((SECONDS + 10))
    while [ -z "${out}" ] && [ "${SECONDS}" -lt "${deadline}" ]; do
        out=$(select_all)
        [ -n "${out}" ] || sleep 0.1
    done
    echo "${out}"
}

echo '-- initial'
select_all

echo 4 >> "${logs_dir}/a.txt"
echo '-- append'
wait_rows

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT file_log_directory_watcher_pause_after_listing"
if ! timeout 10 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT file_log_directory_watcher_pause_after_listing PAUSE"; then
    echo "the watcher did not reach the fail point"
    exit 1
fi
# The paused scan has listed a.txt and not looked at it yet.
mv "${logs_dir}/a.txt" "${logs_dir}/b.txt"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT file_log_directory_watcher_pause_after_listing"
echo 5 >> "${logs_dir}/b.txt"
echo '-- renamed during a scan, then appended'
wait_rows

mv "${logs_dir}/b.txt" "${moved_dir}/b.txt"
printf '10\n20\n' > "${logs_dir}/c.txt"
echo '-- moved out, new file'
wait_rows

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
rm -rf "${logs_dir}" "${moved_dir}"
