#!/usr/bin/env bash
# Test the `clear` client meta-command (clickhouse-local; gated for clickhouse-client non-interactive).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit

echo "-- clear variants exit successfully"
$CLICKHOUSE_LOCAL -q "clear"
$CLICKHOUSE_LOCAL -q "CLEAR"
$CLICKHOUSE_LOCAL -q "/clear"
$CLICKHOUSE_LOCAL -q "/CLEAR;"
echo "OK"

echo "-- clear via --queries-file (same path as -q)"
qf="${CLICKHOUSE_TMP}/04037_clear_queries_file_$$.txt"
printf '%s\n' 'clear' >"$qf"
$CLICKHOUSE_LOCAL --queries-file "$qf"
rm -f "$qf"
echo "OK"

echo "-- clear is not parsed as a bare SQL identifier"
if err="$($CLICKHOUSE_LOCAL -q "clear" 2>&1)"; then
    if echo "$err" | grep -qF 'UNKNOWN_IDENTIFIER'; then
        echo "FAIL: clear was interpreted as SQL: $err" >&2
        exit 1
    fi
else
    echo "FAIL: clear command returned non-zero" >&2
    exit 1
fi
echo "OK"

echo "-- select clear is still SQL"
$CLICKHOUSE_LOCAL -q "select clear" 2>&1 | grep -F 'UNKNOWN_IDENTIFIER' > /dev/null
echo "OK"

echo "-- clickhouse-client -q clear is still SQL (non-interactive)"
set +o errexit
out=$($CLICKHOUSE_CLIENT -q "clear" 2>&1)
rc=$?
set -o errexit
# Must not silently succeed (meta-command without gate). Bare `clear` may fail as
# UNKNOWN_IDENTIFIER (47) after parse or SYNTAX_ERROR (62) at parse time depending on dialect/path.
if [[ $rc -eq 0 ]]; then
    echo "FAIL: clickhouse-client -q clear exited 0 (expected SQL error). Output: ${out}" >&2
    exit 1
fi
if [[ $rc -ne 47 && $rc -ne 62 ]] && ! echo "$out" | grep -qE 'UNKNOWN_IDENTIFIER|SYNTAX_ERROR'; then
    echo "FAIL: expected SQL error (exit 47/62 or UNKNOWN_IDENTIFIER/SYNTAX_ERROR in output), got rc=${rc}: ${out}" >&2
    exit 1
fi
echo "OK"
