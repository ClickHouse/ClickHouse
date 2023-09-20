#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

format="$CLICKHOUSE_FORMAT"

echo "SELECT 1 UNION SELECT 1" | $format
echo "SELECT 2 EXCEPT SELECT 2" | $format
echo "SELECT 3 INTERSECT SELECT 3" | $format
