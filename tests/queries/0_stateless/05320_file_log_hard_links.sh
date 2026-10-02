#!/usr/bin/env bash
# A FileLog file with several names in the directory (hard links, a symbolic link) is read once, under one of
# its names, and keeps being read from the same position whichever of its names it is read under.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${logs_dir}"
mkdir -p "${logs_dir}"/{d1,d2,d3}
d1=${logs_dir}/d1

function read_log()
{
    ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT _filename, id FROM $1 ORDER BY _filename, id" 2>&1
}

# The directory watcher installs its watch asynchronously: append to `$2` until a read observes it.
function sync_watch()
{
    local deadline=$((EPOCHSECONDS + 60)) res=
    while [[ -z "${res}" ]] && ((EPOCHSECONDS < deadline)); do
        printf '0\n' >> "$2"
        res=$(read_log "$1")
        [[ -z "${res}" ]] && sleep 0.5
    done
    grep -v $'\t0$' <<< "${res}"
}

# Reads `$1` until every one of the values `$2...` has been read, then prints the rows read, sorted.
function read_until()
{
    local table=$1
    shift
    local deadline=$((EPOCHSECONDS + 60)) rows= missing v
    while ((EPOCHSECONDS < deadline)); do
        rows+=$(read_log "${table}")$'\n'
        missing=0
        for v in "$@"; do
            grep -q $'\t'"${v}"'$' <<< "${rows}" || missing=1
        done
        ((missing)) || break
        sleep 0.2
    done
    LC_ALL=C sort <<< "${rows}" | sed '/^$/d'
}

printf '1\n2\n' > "${d1}/app.log"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (id UInt64) ENGINE = FileLog('${d1}/', 'TSV')"
read_log file_log
sync_watch file_log "${d1}/app.log"

echo '-- a hard link is not read'
ln "${d1}/app.log" "${d1}/alias.log"
printf '3\n' >> "${d1}/app.log"
read_until file_log 3

echo '-- a write through the other name is read under the read name'
printf '4\n' >> "${d1}/alias.log"
read_until file_log 4

echo '-- the read name is removed and the other name is linked again: read on, not from the start'
rm "${d1}/app.log"
ln "${d1}/alias.log" "${d1}/new.log"
printf '5\n' >> "${d1}/new.log"
read_until file_log 5

echo '-- the same name is linked again'
rm "${d1}/new.log"
ln "${d1}/alias.log" "${d1}/new.log"
printf '6\n' >> "${d1}/new.log"
read_until file_log 6

echo '-- link and unlink: the only other name reads on'
rm "${d1}/alias.log"
ln "${d1}/new.log" "${d1}/next.log"
rm "${d1}/new.log"
printf '7\n' >> "${d1}/next.log"
read_until file_log 7

echo '-- a hard link renamed into the directory is not read'
ln "${d1}/next.log" "${d1}/x.bak"
mv "${d1}/x.bak" "${d1}/x.log"
printf '8\n' >> "${d1}/next.log"
read_until file_log 8

echo '-- the read name is replaced by another file: the other name reads on, the new file is read from the start'
rm "${d1}/x.log"
ln "${d1}/next.log" "${d1}/keep.log"
printf '9\n' > "${d1}/tmp.txt"
mv "${d1}/tmp.txt" "${d1}/next.log"
printf '10\n' >> "${d1}/keep.log"
read_until file_log 9 10

echo '-- a hard link made while the table is detached'
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log"
ln "${d1}/keep.log" "${d1}/a2.log"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log"
printf '11\n' >> "${d1}/keep.log"
read_log file_log

echo '-- hard links that exist when the table is created'
printf '1\n2\n' > "${logs_dir}/d2/a.log"
ln "${logs_dir}/d2/a.log" "${logs_dir}/d2/b.log"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_create (id UInt64) ENGINE = FileLog('${logs_dir}/d2/', 'TSV')"
read_log file_log_create

echo '-- a symbolic link is not read, and is not read either once it dangles'
d3=${logs_dir}/d3
printf '1\n' > "${d3}/a.log"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_symlink (id UInt64) ENGINE = FileLog('${d3}/', 'TSV')"
read_log file_log_symlink
sync_watch file_log_symlink "${d3}/a.log"
ln -s a.log "${d3}/s.log"
printf '2\n' >> "${d3}/a.log"
read_until file_log_symlink 2
rm "${d3}/a.log"
printf '3\n' > "${d3}/b.log"
read_until file_log_symlink 3

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_create"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_symlink"
rm -rf "${logs_dir}"
