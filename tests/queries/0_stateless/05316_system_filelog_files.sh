#!/usr/bin/env bash
# system.filelog_files: one row per file of a FileLog table with its read offset, size, number of records read and
# state; it follows files added to, removed from and renamed in the directory, shows only tables the user may see, and
# shows a table of a lazy_load_tables database once it is loaded.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
mkdir -p "${dir}"
rm -rf "${dir:?}"/*
printf '{"a":1}\n{"a":2}\n{"a":3}\n' > "${dir}/a.jsonl"
printf '{"a":4}\n{"a":5}\n' > "${dir}/b.jsonl"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (a UInt64) ENGINE = FileLog('${dir}/', 'JSONEachRow')"

function files()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT file_name, current_offset, file_size, num_records_read, state, empty(last_exception)
        FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log' ORDER BY file_name"
}

echo '-- before reading'
files
echo '-- after reading'
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT count() FROM file_log"
files
inode=$(ls -i "${dir}/a.jsonl" | awk '{print $1}')
${CLICKHOUSE_CLIENT} -q "SELECT inode = ${inode}, endsWith(path, '/${CLICKHOUSE_TEST_UNIQUE_NAME}/a.jsonl'), last_poll_time > 0
    FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log' AND file_name = 'a.jsonl'"

echo '-- unread bytes'
printf '{"a":6}\n' >> "${dir}/a.jsonl"
${CLICKHOUSE_CLIENT} -q "SELECT file_size - current_offset
    FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log' AND file_name = 'a.jsonl'"

echo '-- a file added and a file removed'
printf '{"a":7}\n' > "${dir}/c.jsonl"
rm "${dir}/b.jsonl"
start=$EPOCHSECONDS
while true; do
    ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT * FROM file_log FORMAT Null"
    done_reading=$(${CLICKHOUSE_CLIENT} -q "SELECT count() = 2 AND sum(current_offset = file_size) = 2 AND has(groupArray(file_name), 'c.jsonl')
        FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log'")
    [ "${done_reading}" = 1 ] && break
    if ((EPOCHSECONDS - start > 120)); then echo "Timeout waiting for the added and removed files"; break; fi
    sleep 0.1
done
files

echo '-- a file recreated with the same inode starts over'
# A hard link outside the watched directory keeps the inode across the unlink, as in
# 04342_file_log_delete_recreate_same_inode; the pause lets the watcher queue both events, so one read processes
# the removal and the re-creation together.
held=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}.held
ln "${dir}/c.jsonl" "${held}"
rm "${dir}/c.jsonl"
printf '{"a":8}\n{"a":9}\n' > "${held}"
ln "${held}" "${dir}/c.jsonl"
rm "${held}"
sleep 1
start=$EPOCHSECONDS
while true; do
    ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT * FROM file_log FORMAT Null"
    done_reading=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.filelog_files
        WHERE database = currentDatabase() AND table = 'file_log' AND file_name = 'c.jsonl' AND current_offset = 16")
    [ "${done_reading}" = 1 ] && break
    if ((EPOCHSECONDS - start > 120)); then echo "Timeout waiting for the recreated file"; break; fi
    sleep 0.1
done
files

echo '-- a renamed file whose name is reused, before the table processes it'
# No materialized view reads this table, so the rename is processed only by the next SELECT: until then the old
# inode's row keeps the name, which now refers to another file.
mv "${dir}/a.jsonl" "${dir}/a.jsonl.1"
printf '{"a":10}\n' > "${dir}/a.jsonl"
${CLICKHOUSE_CLIENT} -q "SELECT file_name, file_size IS NULL
    FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log' ORDER BY file_name"

echo '-- once processed, the renamed file keeps its row and the new file starts from zero'
start=$EPOCHSECONDS
while true; do
    ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT * FROM file_log FORMAT Null"
    done_reading=$(${CLICKHOUSE_CLIENT} -q "SELECT count() = 3 AND sum(current_offset = file_size) = 3 AND has(groupArray(file_name), 'a.jsonl.1')
        FROM system.filelog_files WHERE database = currentDatabase() AND table = 'file_log'")
    [ "${done_reading}" = 1 ] && break
    if ((EPOCHSECONDS - start > 120)); then echo "Timeout waiting for the renamed file"; break; fi
    sleep 0.1
done
files

echo '-- access'
user="user_${CLICKHOUSE_DATABASE}_filelog"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.filelog_files TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT count() FROM system.filelog_files WHERE database = '${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.file_log TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT count() FROM system.filelog_files WHERE database = '${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"

echo '-- a temporary table is not shown'
tmp_dir="${dir}_tmp"
mkdir -p "${tmp_dir}"
printf '{"a":1}\n' > "${tmp_dir}/a.jsonl"
${CLICKHOUSE_CLIENT} -q "
    CREATE TEMPORARY TABLE file_log_tmp (a UInt64) ENGINE = FileLog('${tmp_dir}/', 'JSONEachRow');
    SELECT count() FROM system.tables WHERE is_temporary AND name = 'file_log_tmp';
    SELECT count() FROM system.filelog_files WHERE endsWith(path, '/${CLICKHOUSE_TEST_UNIQUE_NAME}_tmp/a.jsonl');
"

echo '-- a table of a lazy_load_tables database is shown once it is loaded'
lazy_db="${CLICKHOUSE_DATABASE}_lazy"
lazy_dir="${dir}_lazy"
mkdir -p "${lazy_dir}"
printf '{"a":1}\n' > "${lazy_dir}/a.jsonl"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${lazy_db}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${lazy_db} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${lazy_db}.file_log (a UInt64) ENGINE = FileLog('${lazy_dir}/', 'JSONEachRow')"
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${lazy_db}"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${lazy_db}"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${lazy_db}' AND name = 'file_log'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.filelog_files WHERE database = '${lazy_db}'"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT count() FROM ${lazy_db}.file_log"
${CLICKHOUSE_CLIENT} -q "SELECT file_name, current_offset, num_records_read FROM system.filelog_files WHERE database = '${lazy_db}'"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${lazy_db} SYNC"
rm -rf "${lazy_dir:?}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
rm -rf "${dir:?}" "${tmp_dir:?}"
