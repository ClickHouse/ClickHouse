#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

LOG="$CLICKHOUSE_TMP/err-$CLICKHOUSE_DATABASE"

# A loaded runner can need more than the default 10 s `connect_timeout` /
# `handshake_timeout_ms` to accept the connection and send Hello; the benchmark
# then aborts before its final report and prints no summary at all.
$CLICKHOUSE_BENCHMARK --connect_timeout 60 --handshake_timeout_ms 60000 --delay 0 --iterations=10 --reconnect=2 <<< 'SELECT 1' 1>/dev/null 2>"$LOG"

grep -F 'Queries executed' "$LOG" | tail -n1
# Separate grep: the exit status of a `grep | tail` pipeline is `tail`'s, which is
# 0 even when nothing matched, so appending `|| cat` above would never fire.
grep -qF 'Queries executed' "$LOG" || cat "$LOG" >&2

rm "$LOG"
