#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on bzip2

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
WORKING_FOLDER_02003="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"

rm -rf "${WORKING_FOLDER_02003}"
mkdir "${WORKING_FOLDER_02003}"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World!' as String) INTO OUTFILE '${WORKING_FOLDER_02003}/bz2_compression.bz2'"
bzip2 -t ${WORKING_FOLDER_02003}/bz2_compression.bz2
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${WORKING_FOLDER_02003}/bz2_compression.bz2', 'TabSeparated', 'col String')"

rm -rf "${WORKING_FOLDER_02003}"
