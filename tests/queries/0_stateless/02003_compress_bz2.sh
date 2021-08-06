#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World!' as String) INTO OUTFILE '${USER_FILES_PATH}/bz2_compression.bz2'"
bzip2 -t ${USER_FILES_PATH}/bz2_compression.bz2
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${USER_FILES_PATH}/bz2_compression.bz2', 'TabSeparated', 'col String')"

rm -f "${USER_FILES_PATH}/bz2_compression.bz2"
