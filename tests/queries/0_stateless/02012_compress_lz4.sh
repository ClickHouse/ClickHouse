#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
WORKING_FOLDER_02012="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"

rm -rf "${WORKING_FOLDER_02012}"
mkdir "${WORKING_FOLDER_02012}"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World!' as String) INTO OUTFILE '${WORKING_FOLDER_02012}/lz4_compression.lz4'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${WORKING_FOLDER_02012}/lz4_compression.lz4', 'TabSeparated', 'col String')"

rm -rf "${WORKING_FOLDER_02012}"
