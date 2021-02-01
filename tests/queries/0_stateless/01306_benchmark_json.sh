#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_BENCHMARK --iterations 10 --json "${CLICKHOUSE_TMP}"/out.json <<< "SELECT 1" 2>/dev/null && cat "${CLICKHOUSE_TMP}"/out.json |
    $CLICKHOUSE_LOCAL --input-format JSONAsString --structure "s String" --query "SELECT isValidJSON(s) FROM table"
