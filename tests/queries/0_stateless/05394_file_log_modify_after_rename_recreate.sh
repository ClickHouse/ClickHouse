#!/usr/bin/env bash

# A file that was read is renamed, its old name is recreated, and the renamed file is appended to, with no wait in
# between. The renamed file keeps its read position and the recreated one is read from the start, each once: the
# append must not be reported under the old name before the rename is (on macOS kqueue reports the rename and the
# write of the old inode in one event).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}

rm -rf "${logs_dir}"
mkdir -p "${logs_dir}"
printf '1\n2\n3\n' > "${logs_dir}/a.tsv"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE file_log (id Int64) ENGINE = FileLog('${logs_dir}/', 'TSV')"

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

mv "${logs_dir}/a.tsv" "${logs_dir}/b.tsv"
printf '10\n20\n' > "${logs_dir}/a.tsv"
printf '4\n5\n' >> "${logs_dir}/b.tsv"
read_new 4

# Rows read twice would come here.
${CLICKHOUSE_CLIENT} --query "SELECT id FROM file_log ORDER BY id SETTINGS stream_like_engine_allow_direct_select = 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE file_log"
rm -rf "${logs_dir}"
