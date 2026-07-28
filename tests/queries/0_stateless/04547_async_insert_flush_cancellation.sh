#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses a PAUSEABLE failpoint whose channel is global to the server, so concurrent
# test instances would interfere with each other's ENABLE/DISABLE/WAIT sequence.

# Regression test: a background async-insert flush must honor KILL QUERY. Before the fix, the
# parsing loop in StreamingFormatExecutor::execute never polled the cancellation flag, so a flush
# of a buffered payload kept running after KILL and could trip the stress hung check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TABLE="t_async_flush_cancel_${CLICKHOUSE_DATABASE}"
CLIENT_QUERY_ID="async_flush_client_${CLICKHOUSE_DATABASE}_$$"
FP=async_insert_flush_pause_in_executor
CLIENT_OUT="${CLICKHOUSE_TMP}/async_flush_client_$$.out"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query LIKE '%$TABLE%' AND query NOT LIKE '%KILL QUERY%' SYNC FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE" 2>/dev/null ||:
    rm -f "$CLIENT_OUT"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $TABLE (x UInt64) ENGINE = MergeTree ORDER BY x"

# Park the flush inside the executor loop as soon as it starts.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Synchronous async insert over HTTP: the server parses the body (the "Parsed" data kind that goes
# through StreamingFormatExecutor). The request blocks waiting for the background flush, which parks
# at the failpoint. Its own query_id lets us exclude it from the KILL below.
printf '1\n2\n3\n' | ${CLICKHOUSE_CURL} \
    "${CLICKHOUSE_URL}&query_id=${CLIENT_QUERY_ID}&async_insert=1&wait_for_async_insert=1&async_insert_busy_timeout_min_ms=10&async_insert_busy_timeout_max_ms=10&query=INSERT+INTO+${TABLE}+FORMAT+TSV" \
    --data-binary @- > "$CLIENT_OUT" 2>&1 &

# Deterministically wait until the flush has parked at the failpoint (no polling / sleeping).
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"

# Kill ONLY the background flush (query_kind AsyncInsertFlush), not the client's own insert, without
# waiting for it to exit. This sets is_killed on the flush's process-list element.
$CLICKHOUSE_CLIENT -q "
    KILL QUERY WHERE query_kind = 'AsyncInsertFlush' AND query LIKE '%$TABLE%'
    FORMAT Null"

# Release the loop and disable so a resumed iteration skips the failpoint.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

# The synchronous client waits on the same entry the flush was processing; when the flush aborts,
# the entry is finished with the cancellation exception and the client observes it.
wait 2>/dev/null ||:

# Fixed: the parsing loop honored the kill and aborted the flush, so the client sees the executor's
#        own cancellation message ("Format streaming was cancelled").
# Unfixed: the parsing loop ignored the kill; the flush finished parsing and only the downstream
#        pushing pipeline caught the kill, so the client sees the generic "Query was cancelled"
#        instead (empty grep => the reference diff FAILs). For a large payload this is the 20-minute
#        unkillable parse that tripped the stress hung check.
grep -oE "Format streaming was cancelled" "$CLIENT_OUT" | head -1
