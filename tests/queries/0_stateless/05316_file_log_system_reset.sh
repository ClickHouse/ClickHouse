#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: FileLog -> MV streaming latency depends on `BackgroundSchedulePool` scheduling;
# under heavy parallel load `wait_count` can drift past its timeout. Same precedent as `02968_file_log_multiple_read.sh`.
#
# SYSTEM RESET FILELOG makes a FileLog table read all its files again, read one file again from the beginning
# or from an offset, or skip what a file contains now; the new position is kept after DETACH and ATTACH.
# A table that streams to a materialized view re-reads at once, for a directory and for a single file.
# A lazily loaded table (`lazy_load_tables`) can be reset as well.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${logs_dir:?}"
mkdir -p "${logs_dir}"
single_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_single
rm -rf "${single_dir:?}"
mkdir -p "${single_dir}"
lazy_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_lazy
rm -rf "${lazy_dir:?}"
mkdir -p "${lazy_dir}"

printf '{"a":1}\n{"a":2}\n{"a":3}\n' > "${logs_dir}/a.jsonl"
printf '{"a":10}\n' > "${logs_dir}/b.jsonl"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (a UInt64) ENGINE = FileLog('${logs_dir}/', 'JSONEachRow')"

function read_log()
{
    ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 \
        -q "SELECT _filename, _offset, a FROM file_log ORDER BY _filename, _offset"
}

# Prints the number of rows in table $1 once it is $2, or after 120 seconds.
function wait_count()
{
    local start=$EPOCHSECONDS
    while [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $1")" != "$2" ]] && ((EPOCHSECONDS - start < 120)); do
        sleep 0.2
    done
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $1"
}

echo '-- first read'
read_log
echo '-- nothing new'
read_log

echo '-- all files again'
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log"
read_log

echo '-- one file again'
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log FILE 'a.jsonl'"
read_log

echo '-- one file from an offset'
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log FILE 'a.jsonl' OFFSET 8"
read_log

echo '-- skip what the file contains, kept after DETACH and ATTACH'
printf '{"a":4}\n' >> "${logs_dir}/a.jsonl"
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log FILE 'a.jsonl' TO END"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log"
read_log
echo '-- the skipped line'
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log FILE 'a.jsonl' OFFSET 24"
read_log

echo '-- errors'
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log FILE 'c.jsonl'" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log FILE 'a.jsonl' OFFSET 33" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} -q "CREATE TABLE not_file_log (a UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG not_file_log" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo '-- privilege'
user="u_${CLICKHOUSE_DATABASE}"
user_cluster="u_cluster_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}, ${user_cluster}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user_cluster}"
${CLICKHOUSE_CLIENT} -q "GRANT CLUSTER ON *.* TO ${user_cluster}"
${CLICKHOUSE_CLIENT} --user="${user}" -q "SYSTEM RESET FILELOG ${CLICKHOUSE_DATABASE}.file_log" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --user="${user_cluster}" -q "SYSTEM RESET FILELOG ON CLUSTER test_shard_localhost ${CLICKHOUSE_DATABASE}.file_log" 2>&1 \
    | grep -o -m1 'necessary to have the grant SYSTEM RESET FILELOG'
${CLICKHOUSE_CLIENT} -q "GRANT SYSTEM RESET FILELOG ON ${CLICKHOUSE_DATABASE}.file_log TO ${user}"
${CLICKHOUSE_CLIENT} --user="${user}" -q "SYSTEM RESET FILELOG ${CLICKHOUSE_DATABASE}.file_log" && echo 'granted'
echo '-- one file from an offset, ON CLUSTER'
${CLICKHOUSE_CLIENT} -q "GRANT SYSTEM RESET FILELOG ON ${CLICKHOUSE_DATABASE}.file_log TO ${user_cluster}"
${CLICKHOUSE_CLIENT} --user="${user_cluster}" --distributed_ddl_output_mode none \
    -q "SYSTEM RESET FILELOG ON CLUSTER test_shard_localhost ${CLICKHOUSE_DATABASE}.file_log FILE 'a.jsonl' OFFSET 16"
read_log
${CLICKHOUSE_CLIENT} -q "DROP USER ${user}, ${user_cluster}"

echo '-- formatting'
${CLICKHOUSE_CLIENT} -q "EXPLAIN SYNTAX SYSTEM RESET FILELOG db.t ON CLUSTER test_shard_localhost FILE 'x.log' OFFSET 5"
${CLICKHOUSE_CLIENT} -q "EXPLAIN SYNTAX SYSTEM RESET FILELOG db.t FILE 'x.log' TO END"

echo '-- materialized view, directory table'
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst (f String, a UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv TO dst AS SELECT _filename AS f, a FROM file_log"
wait_count dst 5
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log FILE 'b.jsonl'"
wait_count dst 6

echo '-- materialized view, single-file table'
printf '{"a":1}\n{"a":2}\n{"a":3}\n' > "${single_dir}/s.jsonl"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_single (a UInt64) ENGINE = FileLog('${single_dir}/s.jsonl', 'JSONEachRow')
    SETTINGS poll_directory_watch_events_backoff_init = 100, poll_directory_watch_events_backoff_max = 600000,
    poll_directory_watch_events_backoff_factor = 10000"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst_single (a UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_single TO dst_single AS SELECT a FROM file_log_single"
wait_count dst_single 3
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log_single"
wait_count dst_single 6

echo '-- lazily loaded table'
lazy_db="${CLICKHOUSE_DATABASE}_lazy"
printf '{"a":7}\n' > "${lazy_dir}/l.jsonl"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${lazy_db}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${lazy_db} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${lazy_db}.file_log (a UInt64) ENGINE = FileLog('${lazy_dir}/', 'JSONEachRow')"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM ${lazy_db}.file_log"
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${lazy_db}"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${lazy_db}"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${lazy_db}' AND name = 'file_log'"
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG ${lazy_db}.file_log"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a FROM ${lazy_db}.file_log"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${lazy_db} SYNC"

${CLICKHOUSE_CLIENT} -q "DROP TABLE mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE mv_single"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst_single"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_single"
${CLICKHOUSE_CLIENT} -q "DROP TABLE not_file_log"
rm -rf "${logs_dir:?}" "${single_dir:?}" "${lazy_dir:?}"
