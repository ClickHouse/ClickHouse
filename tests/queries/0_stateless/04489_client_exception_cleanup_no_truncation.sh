#!/usr/bin/env bash
# Tags: no-fasttest

# When a query that uses `INTO OUTFILE ... AND STDOUT` fails with an exception in the middle of
# producing its result, the output cleanup must not treat the stopped interrupt handler as a user
# cancellation: the per-query stdout buffer carries a cancellation hook, and on the exception path
# the interrupt handler is already stopped (so its cancelled() is unconditionally true) while the
# query was never actually cancelled. If the stdout buffer were finalized with that handler-based
# hook it would silently discard already-produced output, so the `AND STDOUT` branch would receive
# fewer bytes than the `INTO OUTFILE` branch even though both are fed identically by the same
# ForkWriteBuffer. Assert the invariant that the two branches stay byte-identical through cleanup.
# See https://github.com/ClickHouse/ClickHouse/pull/108078 and the resetOutput() hook re-pointing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUTFILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_exception_cleanup.outfile"
STDOUT_CAPTURE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_exception_cleanup.stdout"

cleanup() { rm -f "$OUTFILE" "$STDOUT_CAPTURE"; }
trap cleanup EXIT
cleanup

# Produce many small blocks (so several are sent before the failure) and throw in the middle. Small
# blocks and disabled limits keep the test cheap and immune to the randomized settings used by the
# flaky check, which could otherwise error out earlier than the throwIf and change the output size.
# The error is expected; only the captured stdout content matters here.
${CLICKHOUSE_CLIENT} --query "
    SELECT number, repeat('x', 50) FROM numbers(1000000)
    WHERE throwIf(number = 500000, 'injected mid-stream failure') = 0
    INTO OUTFILE '${OUTFILE}' AND STDOUT FORMAT TabSeparated
    SETTINGS max_block_size = 8192, max_threads = 4, max_memory_usage = 0,
             max_rows_to_read = 0, max_result_rows = 0, max_result_bytes = 0" \
    > "$STDOUT_CAPTURE" 2>/dev/null

# Both the file and the mirrored stdout are written by the same ForkWriteBuffer, so whatever was
# produced before the failure must appear identically in both - the stdout branch must not be
# truncated by exception cleanup.
if cmp -s "$OUTFILE" "$STDOUT_CAPTURE"
then
    echo "OK: INTO OUTFILE and STDOUT match after exception cleanup"
else
    echo "FAIL: stdout was truncated relative to the outfile during exception cleanup"
    echo "outfile bytes: $(wc -c < "$OUTFILE")  stdout bytes: $(wc -c < "$STDOUT_CAPTURE")"
fi
