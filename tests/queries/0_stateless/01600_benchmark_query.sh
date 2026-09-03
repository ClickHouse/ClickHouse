#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

LOG="$CLICKHOUSE_TMP/err-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_BENCHMARK --iterations 10 --query "SELECT 1" 1>/dev/null 2>"$LOG"

cat "$LOG" | grep Exception
cat "$LOG" | grep Loaded

rm "$LOG"
