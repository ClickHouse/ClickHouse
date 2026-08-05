#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Hello, World!" | $CLICKHOUSE_COMPRESSOR --codec 'Delta' --codec 'LZ4' | $CLICKHOUSE_COMPRESSOR --stat
