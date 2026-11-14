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
    ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT groupArray(a) FROM (SELECT a FROM file_log ORDER BY a)"
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
rm -rf "${base:?}"
