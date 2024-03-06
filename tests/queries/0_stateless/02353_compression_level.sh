#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: depends on brotli and bzip2

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
WORKING_FOLDER_02353="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"

rm -rf "${WORKING_FOLDER_02353}"
mkdir "${WORKING_FOLDER_02353}"

for m in gz br xz zst lz4 bz2
do
    ${CLICKHOUSE_CLIENT} --query "SELECT number, 'Hello, world!' FROM numbers(6000) INTO OUTFILE '${WORKING_FOLDER_02353}/${m}_1.${m}' COMPRESSION '${m}' LEVEL 1"
    ${CLICKHOUSE_CLIENT} --query "SELECT number, 'Hello, world!' FROM numbers(6000) INTO OUTFILE '${WORKING_FOLDER_02353}/${m}_9.${m}' COMPRESSION '${m}' LEVEL 9"

    ${CLICKHOUSE_CLIENT} --query "SELECT count(), max(x), avg(length(s)) FROM file('${WORKING_FOLDER_02353}/${m}_1.${m}', 'TabSeparated', 'x UInt32, s String')"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), max(x), avg(length(s)) FROM file('${WORKING_FOLDER_02353}/${m}_9.${m}', 'TabSeparated', 'x UInt32, s String')"

    level_1=$(du -b ${WORKING_FOLDER_02353}/${m}_1.${m} | awk '{print $1}')
    level_9=$(du -b ${WORKING_FOLDER_02353}/${m}_9.${m} | awk '{print $1}')

    if [ "$level_1" != "$level_9" ]; then
        echo "Ok"
    else
        echo "Failed"
    fi

done

rm -rf "${WORKING_FOLDER_02353}"
