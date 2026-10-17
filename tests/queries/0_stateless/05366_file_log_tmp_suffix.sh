#!/usr/bin/env bash
# A FileLog data file whose name ends with `.tmp` keeps its read position after DETACH and ATTACH.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
single_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_single
rm -rf "${logs_dir:?}" "${single_dir:?}"
mkdir -p "${logs_dir}" "${single_dir}"

# A 252-byte name: a temporary meta file named after it with a suffix would exceed NAME_MAX (255).
long_name=$(printf 'n%.0s' {1..252})

printf '1\n2\n' > "${logs_dir}/app.tmp"
printf '10\n' > "${logs_dir}/app"
printf '100\n' > "${logs_dir}/rot.log"
printf '1000\n' > "${single_dir}/one.tmp"
printf '7\n' > "${single_dir}/${long_name}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (id UInt64) ENGINE = FileLog('${logs_dir}/', 'CSV')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_single (id UInt64) ENGINE = FileLog('${single_dir}/one.tmp', 'CSV')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_long (id UInt64) ENGINE = FileLog('${single_dir}/${long_name}', 'CSV')"

function read_log()
{
    echo "-- $1"
    for table in file_log file_log_single; do
        ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 \
            -q "SELECT _filename, id FROM ${table} ORDER BY _filename, id"
    done
    ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT length(_filename), id FROM file_log_long"
}

function reattach()
{
    for table in "$@"; do
        ${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${table}"
        ${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${table}"
    done
}

read_log "first read"
reattach file_log file_log_single file_log_long
read_log "after ATTACH"

# Renamed to a `.tmp` name while the table is detached: the position moves to the new name.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log"
mv "${logs_dir}/rot.log" "${logs_dir}/rot.log.tmp"
printf '200\n' >> "${logs_dir}/rot.log.tmp"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log"
read_log "after renaming rot.log to rot.log.tmp"
reattach file_log file_log_single file_log_long
read_log "after ATTACH"

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_single"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_long"
rm -rf "${logs_dir:?}" "${single_dir:?}"
