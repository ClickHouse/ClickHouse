#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

format="$CLICKHOUSE_FORMAT --oneline"

echo "SELECT a + b AS x, a + b AS x" | $format
echo "SELECT a + b AS x, a + c AS x" | $format
echo "SELECT a + b AS x, x" | $format
