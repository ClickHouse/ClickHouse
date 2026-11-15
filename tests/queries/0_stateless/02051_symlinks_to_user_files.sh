#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_PATH="${USER_FILES_PATH}/file"
mkdir -p ${FILE_PATH}
chmod 777 ${FILE_PATH}

FILE="test_symlink_${CLICKHOUSE_DATABASE}"

symlink_path=${FILE_PATH}/${FILE}
symlink_path_with_regex="${FILE_PATH}*/${FILE}"
file_path=$CUR_DIR/${FILE}

touch ${file_path}
ln -s ${file_path} ${symlink_path}
chmod ugo+w ${symlink_path}

function cleanup()
{
    rm ${symlink_path} ${file_path}
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query="insert into table function file('${symlink_path}', 'Values', 'a String') select 'OK'";
${CLICKHOUSE_CLIENT} --query="select * from file('${symlink_path}', 'Values', 'a String') order by a";
${CLICKHOUSE_CLIENT} --query="select * from file('${symlink_path_with_regex}', 'Values', 'a String') order by a";
