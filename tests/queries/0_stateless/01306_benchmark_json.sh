#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_BENCHMARK --iterations 10 --json "${CLICKHOUSE_TMP}"/out.json <<< "SELECT 1" 2>/dev/null && cat "${CLICKHOUSE_TMP}"/out.json |
    $CLICKHOUSE_LOCAL --input-format JSONAsString --structure "s String" --query "SELECT isValidJSON(s) FROM table"
