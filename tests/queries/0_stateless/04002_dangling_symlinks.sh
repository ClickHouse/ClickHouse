#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TEST_DIR_NAME=test_04002
DATA_DIR=${USER_FILES_PATH:?}/$TEST_DIR_NAME

mkdir -p $DATA_DIR

echo "data" >> ${DATA_DIR}/good.txt
ln -s ${DATA_DIR}/that_does_not_exist ${DATA_DIR}/broken_link

$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$DATA_DIR/**', 'LineAsString')"

rm -rf $DATA_DIR
