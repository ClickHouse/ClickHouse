#!/usr/bin/env bash
# Tags: no-darwin
# Tag no-darwin: the macOS watcher reports a replaced symlink target as directory events.

# A FileLog directory table whose symlinks swap or rotate their targets outside the directory reads each file on from
# its own offset.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

base=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${base:?}"
mkdir -p "${base}/dir" "${base}/outside"
printf '100\n101\n102\n103\n' > "${base}/outside/x"
echo 200 > "${base}/outside/y"
ln -s "${base}/outside/x" "${base}/dir/a.csv"
ln -s "${base}/outside/y" "${base}/dir/b.csv"

function read_table()
{
    ${CLICKHOUSE_CLIENT} --send_logs_level=fatal --stream_like_engine_allow_direct_select=1 -q "SELECT groupArray(a) FROM (SELECT a FROM ${1:-file_log} ORDER BY a)"
}

${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (a UInt64) ENGINE = FileLog('${base}/dir/', 'CSV')"
read_table

# Appended while detached, swapped after the attach and before the first read.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log"
echo 104 >> "${base}/outside/x"
echo 201 >> "${base}/outside/y"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log"
mv "${base}/outside/x" "${base}/outside/t"
mv "${base}/outside/y" "${base}/outside/x"
mv "${base}/outside/t" "${base}/outside/y"
read_table

# a.csv leads to a new file, b.csv to the old file of a.csv.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log"
echo 202 >> "${base}/outside/x"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log"
mv "${base}/outside/y" "${base}/outside/y.1"
mv "${base}/outside/x" "${base}/outside/y"
echo 300 > "${base}/outside/x"
read_table

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"

# b.csv leads to no file while a.csv is read, then a.csv leads to the old file of b.csv and b.csv to the read one.
mkdir -p "${base}/dir2"
printf '400\n401\n' > "${base}/outside/u"
printf '500\n501\n' > "${base}/outside/v"
ln -s "${base}/outside/u" "${base}/dir2/a.csv"
ln -s "${base}/outside/v" "${base}/dir2/b.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_2 (a UInt64) ENGINE = FileLog('${base}/dir2/', 'CSV')"
mv "${base}/outside/v" "${base}/outside/v.1"
read_table file_log_2
mv "${base}/outside/u" "${base}/outside/v"
mv "${base}/outside/v.1" "${base}/outside/u"
read_table file_log_2
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_2"

# After an attach, a.csv takes over the file of b.csv while b.csv leads to no file, which then gets the old file of a.csv
# back. As for any name that led to no file, b.csv reads it from the start (its inode number may belong to a new file).
mkdir -p "${base}/dir3"
printf '600\n601\n' > "${base}/outside/p"
printf '700\n701\n' > "${base}/outside/q"
ln -s "${base}/outside/p" "${base}/dir3/a.csv"
ln -s "${base}/outside/q" "${base}/dir3/b.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_3 (a UInt64) ENGINE = FileLog('${base}/dir3/', 'CSV')"
read_table file_log_3
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log_3"
echo 602 >> "${base}/outside/p"
echo 702 >> "${base}/outside/q"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log_3"
mv "${base}/outside/p" "${base}/outside/t"
mv "${base}/outside/q" "${base}/outside/p"
read_table file_log_3
mv "${base}/outside/t" "${base}/outside/q"
read_table file_log_3
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_3"

# A rotation of three files where the name in the middle, b.csv, is not read at that time: c.csv takes over the file of
# b.csv, which makes b.csv retried, and b.csv takes over the old file of a.csv before a.csv is re-keyed to its new file.
mkdir -p "${base}/dir4"
printf '800\n801\n802\n803\n' > "${base}/outside/k"
echo 900 > "${base}/outside/l"
echo 950 > "${base}/outside/m"
ln -s "${base}/outside/k" "${base}/dir4/a.csv"
ln -s "${base}/outside/l" "${base}/dir4/b.csv"
ln -s "${base}/outside/m" "${base}/dir4/c.csv"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_4 (a UInt64) ENGINE = FileLog('${base}/dir4/', 'CSV')"
read_table file_log_4
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log_4 FILE 'a.csv' OFFSET 16"
${CLICKHOUSE_CLIENT} -q "SYSTEM RESET FILELOG file_log_4 FILE 'c.csv' OFFSET 4"
echo 804 >> "${base}/outside/k"
echo 901 >> "${base}/outside/l"
mv "${base}/outside/m" "${base}/outside/m.1"
mv "${base}/outside/l" "${base}/outside/m"
mv "${base}/outside/k" "${base}/outside/l"
echo 990 > "${base}/outside/k"
read_table file_log_4
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_4"
rm -rf "${base:?}"
