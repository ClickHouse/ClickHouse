#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$USER_FILES_PATH/test_02125.data

echo "number=1" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('test_02125.data', 'TSKV', 'number UInt64') settings max_read_buffer_size=3, input_format_parallel_parsing=0"
