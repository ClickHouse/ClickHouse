#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Hello, World!" > 03260_test_data

$CLICKHOUSE_COMPRESSOR --codec 'Delta' --codec 'LZ4' --input '03260_test_data' --output '03260_test_out'

$CLICKHOUSE_COMPRESSOR --stat '03260_test_out'

rm -f 03260_test_data 03260_test_out
