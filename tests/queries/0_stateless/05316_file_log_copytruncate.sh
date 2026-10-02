#!/usr/bin/env bash
# A FileLog file truncated in place (as `logrotate` does with `copytruncate`) is read again from offset 0,
# and the other files of the table keep being read.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${logs_dir}"
mkdir -p "${logs_dir}"

printf '1\n2\n3\n' > "${logs_dir}/app.log"
printf '10\n' > "${logs_dir}/other.log"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log (id UInt64) ENGINE = FileLog('${logs_dir}/', 'CSV')"

function read_log()
{
    ${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 \
        -q "SELECT _filename, _offset, id FROM file_log ORDER BY _filename, _offset" 2>&1
}

read_log

# `>` truncates in place and keeps the inode. While the table is detached, app.log becomes shorter
# than the offset already read from it, and other.log grows.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log"
printf '4\n40\n' > "${logs_dir}/app.log"
printf '20\n' >> "${logs_dir}/other.log"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log"
read_log

# The same while the table is attached. The directory watcher reports the change asynchronously,
# so read until it has been observed.
printf '5\n' > "${logs_dir}/app.log"
deadline=$((EPOCHSECONDS + 60))
res=
while [[ -z "${res}" ]] && ((EPOCHSECONDS < deadline)); do
    res=$(read_log)
    [[ -z "${res}" ]] && sleep 0.5
done
echo "${res}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log"
rm -rf "${logs_dir}"
