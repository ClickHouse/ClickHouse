#!/usr/bin/env bash

# A file renamed into a FileLog directory from elsewhere is a new file and is read from the start, even when it
# has the inode of a file just deleted from the directory. A hard link outside the directory keeps the inode here;
# on ext4 a freed inode is often given to the next new file the same way. A rename inside the directory keeps the
# read position.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
# Outside the watched directory, on the same filesystem (a hard link cannot cross filesystems).
held=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}.held

rm -rf "${logs_dir}" "${held}"
mkdir -p "${logs_dir}"
printf '1\n2\n3\n' > "${logs_dir}/a.csv"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE file_log (id Int64) ENGINE = FileLog('${logs_dir}/', 'CSV')"

# The directory watcher reports changes asynchronously, so read until at least $1 new rows arrived.
function read_new()
{
    local expected=$1 rows=""
    for _ in {1..60}; do
        rows+=$(${CLICKHOUSE_CLIENT} --query "SELECT id FROM file_log SETTINGS stream_like_engine_allow_direct_select = 1")$'\n'
        [[ $(grep -c . <<< "${rows}") -ge ${expected} ]] && break
        sleep 0.5
    done
    grep . <<< "${rows}" | sort -n
}

read_new 3

ln "${logs_dir}/a.csv" "${held}"
rm "${logs_dir}/a.csv"
printf '10\n20\n30\n40\n' > "${held}"
mv "${held}" "${logs_dir}/b.csv"
read_new 4

mv "${logs_dir}/b.csv" "${logs_dir}/c.csv"
printf '50\n' >> "${logs_dir}/c.csv"
read_new 1

${CLICKHOUSE_CLIENT} --query "DROP TABLE file_log"
rm -rf "${logs_dir}" "${held}"
