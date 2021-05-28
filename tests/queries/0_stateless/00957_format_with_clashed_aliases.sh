#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

format="$CLICKHOUSE_FORMAT"

echo "SELECT 1 AS x, x.y FROM (SELECT 'Hello, world' AS y) AS x" | $format
