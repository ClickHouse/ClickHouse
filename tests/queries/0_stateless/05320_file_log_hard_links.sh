#!/usr/bin/env bash
# A FileLog file with several names in the directory (hard links, symbolic links) is read once, under one of
# its names, and keeps being read from the same position whichever of its names it is read under.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${logs_dir}"
mkdir -p "${logs_dir}"/{d1,d2,d3}
d1=${logs_dir}/d1
held=${logs_dir}/held.log
held2=${logs_dir}/held2.log
out=${logs_dir}/out.log
tmp=${logs_dir}/tmp.txt

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
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (id UInt64) ENGINE = FileLog('${d1}/', 'TSV') SETTINGS poll_timeout_ms = 100"
read_log file_log
sync_watch file_log "${d1}/app.log"

echo '-- a hard link is not read'
ln "${d1}/app.log" "${d1}/alias.log"
printf '3\n' >> "${d1}/app.log"
read_until file_log 3

echo '-- a write through the other name is read under the read name'
printf '4\n' >> "${d1}/alias.log"
read_until file_log 4

echo '-- the read name is removed, and the other name is linked again and then removed: read on, not from the start'
rm "${d1}/app.log"
ln "${d1}/alias.log" "${d1}/new.log"
rm "${d1}/alias.log"
printf '5\n' >> "${d1}/new.log"
read_until file_log 5

echo '-- the same name is linked again'
ln "${d1}/new.log" "${d1}/alias.log"
rm "${d1}/new.log"
ln "${d1}/alias.log" "${d1}/new.log"
printf '6\n' >> "${d1}/new.log"
read_until file_log 6 | cut -f2

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
printf '9\n' > "${tmp}"
mv "${tmp}" "${d1}/next.log"
printf '10\n' >> "${d1}/keep.log"
read_until file_log 9 10

echo '-- a hard link re-linked to another file: a write through it is read under that file'
ln "${d1}/keep.log" "${d1}/x.log"
printf '12\n' >> "${d1}/x.log"
read_until file_log 12
printf '13\n' >> "${d1}/x.log"
rm "${d1}/x.log"
ln "${d1}/next.log" "${d1}/x.log"
printf '14\n' >> "${d1}/x.log"
read_until file_log 13 14

echo '-- a hard link made while the table is detached'
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log"
ln "${d1}/keep.log" "${d1}/a2.log"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log"
printf '11\n' >> "${d1}/keep.log"
read_log file_log

echo '-- hard links and a symbolic link that exist when the table is created'
printf '1\n2\n' > "${logs_dir}/d2/a.log"
ln "${logs_dir}/d2/a.log" "${logs_dir}/d2/b.log"
ln -s a.log "${logs_dir}/d2/0.log"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_create (id UInt64) ENGINE = FileLog('${logs_dir}/d2/', 'TSV') SETTINGS poll_timeout_ms = 100"
read_log file_log_create

echo '-- a symbolic link that exists when the table is created'
d3=${logs_dir}/d3
printf '1\n' > "${d3}/a.log"
ln -s a.log "${d3}/s1.log"
: > "${d3}/sync.log"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_symlink (id UInt64) ENGINE = FileLog('${d3}/', 'TSV') SETTINGS poll_timeout_ms = 100"
read_log file_log_symlink
sync_watch file_log_symlink "${d3}/sync.log"

echo '-- hard links and symbolic links to a read file are not read'
ln "${d3}/a.log" "${d3}/b.log"
ln -s b.log "${d3}/1.log"
ln -s b.log "${d3}/tmp.sym"
mv "${d3}/tmp.sym" "${d3}/0.log"
printf '2\n' >> "${d3}/a.log"
read_until file_log_symlink 2

echo '-- the read name is removed: a hard link reads on, not a symbolic link'
rm "${d3}/a.log"
printf '3\n' >> "${d3}/b.log"
read_until file_log_symlink 3

echo '-- symbolic links that no longer point to the file do not keep it: a file linked back from outside the directory is read from the start'
ln "${d3}/b.log" "${held}"
rm "${d3}/b.log"
ln "${held}" "${d3}/v.log"
printf '4\n' >> "${d3}/v.log"
read_until file_log_symlink 1 2 3 4

echo '-- a symbolic link to a file outside the directory is read'
printf '5\n' > "${out}"
ln -s ../out.log "${d3}/z.log"
read_until file_log_symlink 5

echo '-- a hard link to a file read under a symbolic link reads it on'
ln "${out}" "${d3}/y.log"
printf '6\n' >> "${d3}/y.log"
read_until file_log_symlink 6

echo '-- the hard link is removed: the symbolic link reads on'
rm "${d3}/y.log"
printf '7\n' >> "${out}"
read_until file_log_symlink 7

echo '-- a symbolic link that no longer points to the file does not keep it either when it is the read name'
mv "${out}" "${logs_dir}/out2.log"
ln "${logs_dir}/out2.log" "${d3}/w.log"
printf '8\n' >> "${d3}/w.log"
read_until file_log_symlink 5 6 7 8

echo '-- a symbolic link renamed over the read name: a hard link of the file reads on, also after the link target is removed'
ln "${d3}/w.log" "${d3}/h.log"
ln "${d3}/w.log" "${d3}/k.log"
ln -s h.log "${d3}/tmp.sym"
mv "${d3}/tmp.sym" "${d3}/w.log"
printf '9\n' >> "${d3}/h.log"
read_until file_log_symlink 9
rm "${d3}/h.log"
printf '10\n' >> "${d3}/k.log"
read_until file_log_symlink 10

echo '-- the read name is removed: a hard link reads on before a symbolic link to the file outside the directory'
ln -s ../out2.log "${d3}/a2.log"
ln "${d3}/k.log" "${d3}/m.log"
rm "${d3}/k.log"
printf '11\n' >> "${d3}/m.log"
read_until file_log_symlink 11

echo '-- a symbolic link to the read name does not keep the file: the name re-created with the same inode is read from the start'
printf '20\n' > "${d3}/r.log"
read_until file_log_symlink 20
ln -s r.log "${d3}/cur.log"
ln "${d3}/r.log" "${held2}"
rm "${d3}/r.log"
printf '21\n22\n23\n' > "${held2}"
ln "${held2}" "${d3}/r.log"
read_until file_log_symlink 21 22 23

echo '-- a symbolic link out of the directory that no longer points to the file does not keep it: the name linked back is read from the start'
mv "${logs_dir}/out2.log" "${logs_dir}/out3.log"
rm "${d3}/m.log"
ln "${logs_dir}/out3.log" "${d3}/m.log"
printf '12\n' >> "${d3}/m.log"
read_until file_log_symlink 5 6 7 8 9 10 11 12

echo '-- a symbolic link to a symbolic link out of the directory follows that name: the name re-created is read from the start'
printf '30\n' > "${logs_dir}/out4.log"
ln -s ../out4.log "${d3}/q.log"
read_until file_log_symlink 30
ln -s q.log "${d3}/p.log"
rm "${d3}/q.log"
printf '31\n32\n33\n' > "${logs_dir}/out4.log"
ln -s ../out4.log "${d3}/q.log"
read_until file_log_symlink 31 32 33

echo '-- symbolic links made while the table is detached: the link out of the directory is read, and the link to it follows its name'
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log_symlink"
printf '40\n' > "${logs_dir}/out5.log"
ln -s ../out5.log "${d3}/u.log"
ln -s u.log "${d3}/t.log"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log_symlink"
read_until file_log_symlink 40
sync_watch file_log_symlink "${d3}/sync.log"
rm "${d3}/u.log"
printf '41\n42\n43\n' > "${logs_dir}/out5.log"
ln -s ../out5.log "${d3}/u.log"
read_until file_log_symlink 41 42 43

echo '-- a symbolic link out of the directory renamed over the read name: a hard link of the file reads on'
printf '50\n' > "${logs_dir}/out6.log"
ln "${logs_dir}/out6.log" "${d3}/e.log"
read_until file_log_symlink 50
ln "${d3}/e.log" "${d3}/f.log"
ln -s ../out6.log "${d3}/tmp.sym"
mv "${d3}/tmp.sym" "${d3}/e.log"
printf '51\n' >> "${d3}/f.log"
read_until file_log_symlink 51

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_create"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_symlink"
rm -rf "${logs_dir}"
