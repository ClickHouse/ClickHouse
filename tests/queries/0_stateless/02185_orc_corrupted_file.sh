#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cp $CUR_DIR/data_orc/corrupted.orc $USER_FILES_PATH/

${CLICKHOUSE_CLIENT} --query="select * from file('corrupted.orc')" 2>&1 | grep -F -q 'CANNOT_EXTRACT_TABLE_STRUCTURE' && echo 'OK' || echo 'FAIL'
