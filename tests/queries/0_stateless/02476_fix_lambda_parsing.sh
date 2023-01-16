#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

format="$CLICKHOUSE_FORMAT"

echo "SELECT f(x, (y) -> z)" | $format
echo "SELECT f(x, y -> z)" | $format
echo "SELECT f((x, y) -> z)" | $format
echo "SELECT f(x, (x, y) -> z)" | $format
