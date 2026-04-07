#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER_02012="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"

rm -rf "${WORKING_FOLDER_02012}"
mkdir "${WORKING_FOLDER_02012}"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World!' as String) INTO OUTFILE '${WORKING_FOLDER_02012}/lz4_compression.lz4'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${WORKING_FOLDER_02012}/lz4_compression.lz4', 'TabSeparated', 'col String')"

rm -rf "${WORKING_FOLDER_02012}"
