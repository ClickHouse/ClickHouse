#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_USER_FILES_UNIQUE/test_02125.data

echo "number=1" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/test_02125.data', 'TSKV', 'number UInt64') settings max_read_buffer_size=3, input_format_parallel_parsing=0"
