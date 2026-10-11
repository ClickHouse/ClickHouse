#!/usr/bin/env bash
# A relative FileLog path is resolved against user_files_path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${USER_FILES_PATH:?}/${dir}"
mkdir -p "${USER_FILES_PATH}/${dir}"
printf '1\n2\n3\n' > "${USER_FILES_PATH}/${dir}/a.csv"
printf '10\n' > "${USER_FILES_PATH}/${dir}/b.csv"

# Directory
${CLICKHOUSE_CLIENT} --query "CREATE TABLE file_log_dir (k UInt64) ENGINE = FileLog('${dir}/', 'CSV')"
${CLICKHOUSE_CLIENT} --query "SELECT k FROM file_log_dir ORDER BY k SETTINGS stream_like_engine_allow_direct_select = 1"
# The table definition keeps the path as written.
${CLICKHOUSE_CLIENT} --query "SELECT replace(engine_full, '${dir}', 'DIR') FROM system.tables WHERE database = currentDatabase() AND name = 'file_log_dir'"
# Reattached table reads on from the stored offsets in the same directory.
echo 4 >> "${USER_FILES_PATH}/${dir}/a.csv"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE file_log_dir"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE file_log_dir"
${CLICKHOUSE_CLIENT} --query "SELECT k FROM file_log_dir ORDER BY k SETTINGS stream_like_engine_allow_direct_select = 1"

# Single file
${CLICKHOUSE_CLIENT} --query "CREATE TABLE file_log_file (k UInt64) ENGINE = FileLog('${dir}/b.csv', 'CSV')"
${CLICKHOUSE_CLIENT} --query "SELECT k FROM file_log_file ORDER BY k SETTINGS stream_like_engine_allow_direct_select = 1"

# A relative path cannot leave user_files_path.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE file_log_escape (k UInt64) ENGINE = FileLog('${dir}/../../', 'CSV')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

${CLICKHOUSE_CLIENT} --query "DROP TABLE file_log_dir"
${CLICKHOUSE_CLIENT} --query "DROP TABLE file_log_file"
rm -rf "${USER_FILES_PATH:?}/${dir}"
