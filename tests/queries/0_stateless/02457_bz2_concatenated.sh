#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on bzip2

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER_02457="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"

rm -rf "${WORKING_FOLDER_02457}"
mkdir "${WORKING_FOLDER_02457}"


${CLICKHOUSE_CLIENT} --query "SELECT * FROM numbers(0, 2) INTO OUTFILE '${WORKING_FOLDER_02457}/file_1.bz2'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM numbers(2, 2) INTO OUTFILE '${WORKING_FOLDER_02457}/file_2.bz2'"
cat ${WORKING_FOLDER_02457}/file_1.bz2 ${WORKING_FOLDER_02457}/file_2.bz2 > ${WORKING_FOLDER_02457}/concatenated.bz2
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${WORKING_FOLDER_02457}/concatenated.bz2', 'TabSeparated', 'col Int64')"

rm -rf "${WORKING_FOLDER_02457}"
