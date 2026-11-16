#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

LOG="$CLICKHOUSE_TMP/err-$CLICKHOUSE_DATABASE"
# On slow lanes (amd_msan + WasmEdge under parallel load) the server can need
# more than the default 10 s handshake_timeout_ms to send Hello; the resulting
# SOCKET_TIMEOUT NetException then leaks into stderr and trips `grep Exception`.
# Give connect + handshake a generous budget so the run stays deterministic.
$CLICKHOUSE_BENCHMARK --connect_timeout 60 --handshake_timeout_ms 60000 --iterations 10 --query "SELECT 1" 1>/dev/null 2>"$LOG"

cat "$LOG" | grep Exception
cat "$LOG" | grep Loaded

rm "$LOG"
