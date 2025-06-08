#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER_02357="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"

rm -rf "${WORKING_FOLDER_02357}"
mkdir "${WORKING_FOLDER_02357}"

for i in 0 2 5 6 7 9
do
    echo "text_${i}" > "${WORKING_FOLDER_02357}/file_${i}"
done

${CLICKHOUSE_CLIENT} --query "WITH '${WORKING_FOLDER_02357}/file_' || toString(number) AS path SELECT file(path, 'default'), file(path, NULL) from numbers(10);"
${CLICKHOUSE_CLIENT} --query "WITH '${WORKING_FOLDER_02357}/file_' || toString(number) AS path SELECT file(path, 'default'), file(path, NULL) from numbers(3, 10);"

rm -rf "${WORKING_FOLDER_02357}"
