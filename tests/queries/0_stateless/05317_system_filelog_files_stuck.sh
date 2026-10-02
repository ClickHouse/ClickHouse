#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the FileLog -> MV streaming path depends on `BackgroundSchedulePool` task scheduling latency, and
# under heavy parallel load pool contention can push file detection past any timeout (as in 02968_file_log_multiple_read).

# system.filelog_files shows a file as stuck, with the exception, when a materialized view fails to consume it or a round
# fails before reading it, and as consuming again once a later read of the file succeeds.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
mkdir -p "${dir}"
rm -rf "${dir:?}"/*
printf '{"a":0}\n' > "${dir}/a.jsonl"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE file_log (a UInt64) ENGINE = FileLog('${dir}/', 'JSONEachRow') SETTINGS poll_directory_watch_events_backoff_max = 1000;
    CREATE TABLE dst (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT a FROM file_log WHERE throwIf(a = 0, 'zero record') = 0;
"

function wait_for_state()
{
    local start=$EPOCHSECONDS
    until [ "$(${CLICKHOUSE_CLIENT} -q "SELECT state FROM system.filelog_files WHERE database = currentDatabase() AND table = '${2:-file_log}'")" = "$1" ]; do
        if ((EPOCHSECONDS - start > 120)); then echo "Timeout waiting for state $1"; exit 1; fi
        sleep 0.5
    done
}

wait_for_state stuck
${CLICKHOUSE_CLIENT} -q "SELECT state, last_exception LIKE '%FUNCTION_THROW_IF_VALUE_IS_NON_ZERO%', last_exception_time > 0, num_records_read, current_offset
    FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log'"

printf '{"a":1}\n' >> "${dir}/a.jsonl"
wait_for_state consuming
${CLICKHOUSE_CLIENT} -q "SELECT state, last_exception LIKE '%FUNCTION_THROW_IF_VALUE_IS_NON_ZERO%', num_records_read, current_offset
    FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log'"
${CLICKHOUSE_CLIENT} -q "SELECT a FROM dst"

echo '-- a round that fails before it reads the file'
dir_2="${dir}_2"
mkdir -p "${dir_2}"
rm -rf "${dir_2:?}"/*
printf '{"a":2}\n' > "${dir_2}/b.jsonl"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE file_log_2 (a UInt64) ENGINE = FileLog('${dir_2}/', 'JSONEachRow') SETTINGS poll_directory_watch_events_backoff_max = 1000;
    CREATE TABLE dst_2 (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv_2 TO dst_2 AS SELECT a FROM file_log_2;
"
start=$EPOCHSECONDS
until [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM dst_2")" = 1 ]; do
    if ((EPOCHSECONDS - start > 120)); then echo "Timeout waiting for the first record"; exit 1; fi
    sleep 0.5
done
# A destination that rejects inserts makes every later round fail before it reads the appended record.
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst_2; CREATE TABLE dst_2 (a UInt64) ENGINE = Merge(currentDatabase(), '^nothing$')"
printf '{"a":3}\n' >> "${dir_2}/b.jsonl"
wait_for_state stuck file_log_2
${CLICKHOUSE_CLIENT} -q "SELECT state, last_exception LIKE '%NOT_IMPLEMENTED%', file_size - current_offset
    FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log_2'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE mv_2; DROP TABLE dst_2; DROP TABLE file_log_2"
rm -rf "${dir_2:?}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE mv; DROP TABLE dst; DROP TABLE file_log"
rm -rf "${dir:?}"
