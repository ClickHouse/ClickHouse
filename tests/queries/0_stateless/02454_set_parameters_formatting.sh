#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

format="$CLICKHOUSE_FORMAT"

echo "set param_a = 1" | $format
echo "set max_threads = 1, param_a = 1" | $format
echo "set param_a = 1, max_threads = 1" | $format
