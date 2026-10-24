#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the FileLog -> MV streaming path depends on `BackgroundSchedulePool` task scheduling latency, and
# under heavy parallel load pool contention can push file detection past any timeout (as in 02968_file_log_multiple_read).

# system.filelog_files shows a file as stuck, with the exception, when a materialized view fails to consume it or a round
# fails before reading it (also after the file was truncated), and as consuming again once a later read of the file succeeds.

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

function wait_for_file_state()
{
    local start=$EPOCHSECONDS
    until [ "$(${CLICKHOUSE_CLIENT} -q "SELECT state FROM system.filelog_files
        WHERE database = currentDatabase() AND table = '$1' AND file_name = '$2'")" = "$3" ]; do
        if ((EPOCHSECONDS - start > 120)); then echo "Timeout waiting for $2 to be $3"; exit 1; fi
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

echo '-- a file truncated in place while the rounds fail before reading it'
# x.jsonl keeps every round busy, so the event of the truncated c.jsonl is never applied and only its size tells.
dir_3="${dir}_3"
mkdir -p "${dir_3}"
rm -rf "${dir_3:?}"/*
printf '{"a":3}\n{"a":4}\n' > "${dir_3}/c.jsonl"
printf '{"a":5}\n' > "${dir_3}/x.jsonl"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE file_log_3 (a UInt64) ENGINE = FileLog('${dir_3}/', 'JSONEachRow') SETTINGS poll_directory_watch_events_backoff_max = 1000;
    CREATE TABLE dst_3 (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv_3 TO dst_3 AS SELECT a FROM file_log_3;
"
start=$EPOCHSECONDS
until [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM dst_3")" = 3 ]; do
    if ((EPOCHSECONDS - start > 120)); then echo "Timeout waiting for the first records"; exit 1; fi
    sleep 0.5
done
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst_3; CREATE TABLE dst_3 (a UInt64) ENGINE = Merge(currentDatabase(), '^nothing$')"
printf '{"a":6}\n' >> "${dir_3}/x.jsonl"
wait_for_file_state file_log_3 x.jsonl stuck
printf '{"a":7}\n' > "${dir_3}/c.jsonl"
wait_for_file_state file_log_3 c.jsonl stuck
${CLICKHOUSE_CLIENT} -q "SELECT file_name, state, last_exception LIKE '%NOT_IMPLEMENTED%', current_offset, file_size
    FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log_3' ORDER BY file_name"
${CLICKHOUSE_CLIENT} -q "DROP TABLE mv_3; DROP TABLE dst_3; DROP TABLE file_log_3"
rm -rf "${dir_3:?}"

echo '-- a file that a failed round marked stuck and that could not be opened is consuming once it can be read'
# The first round fails on bad.jsonl while the symlink target of link.jsonl is gone, so both files are stuck.
dir_4="${dir}_4"
target_4="${dir}_4_target.jsonl"
mkdir -p "${dir_4}"
rm -rf "${dir_4:?}"/*
printf '{"a":0}\n' > "${dir_4}/bad.jsonl"
printf '{"a":7}\n' > "${target_4}"
ln -s "${target_4}" "${dir_4}/link.jsonl"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_4 (a UInt64) ENGINE = FileLog('${dir_4}/', 'JSONEachRow')
    SETTINGS poll_directory_watch_events_backoff_init = 500, poll_directory_watch_events_backoff_max = 1000"
mv "${target_4}" "${target_4}.old"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE dst_4 (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW mv_4 TO dst_4 AS SELECT a FROM file_log_4 WHERE throwIf(a = 0, 'zero record') = 0;
"
wait_for_file_state file_log_4 bad.jsonl stuck
wait_for_file_state file_log_4 link.jsonl stuck
mv "${target_4}.old" "${target_4}"
start=$EPOCHSECONDS
until [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM dst_4")" = 1 ]; do
    if ((EPOCHSECONDS - start > 120)); then echo "Timeout waiting for the record of link.jsonl"; exit 1; fi
    sleep 0.5
done
wait_for_file_state file_log_4 link.jsonl consuming
${CLICKHOUSE_CLIENT} -q "SELECT file_name, state FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log_4' ORDER BY file_name"
${CLICKHOUSE_CLIENT} -q "SELECT a FROM dst_4"
${CLICKHOUSE_CLIENT} -q "DROP TABLE mv_4; DROP TABLE dst_4; DROP TABLE file_log_4"
rm -rf "${dir_4:?}" "${target_4}" "${target_4}.old"

${CLICKHOUSE_CLIENT} -q "DROP TABLE mv; DROP TABLE dst; DROP TABLE file_log"
rm -rf "${dir:?}"
