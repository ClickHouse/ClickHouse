#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Wait for the metric collector to gather at least one sample
# (collect_interval_milliseconds defaults to 1000ms).
$CLICKHOUSE_CLIENT --query "SELECT sleep(2) FORMAT Null"

# Attempt an explicit flush of metric_log. Under sanitizers with
# distributed cache the server-side 180s timeout may fire before the
# wide metric_log (1700+ columns) finishes writing.  Only suppress that
# specific error so that real regressions still fail the test.
flush_output=$($CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS metric_log" 2>&1)
flush_rc=$?
if [[ $flush_rc -ne 0 ]]; then
    if echo "$flush_output" | grep -qF "TIMEOUT_EXCEEDED"; then
        # Expected timeout under sanitizers — fall through to polling
        :
    else
        # Unexpected error — fail the test
        echo "$flush_output" >&2
        exit 1
    fi
fi

# Poll for data: the background flush thread will eventually write the
# collected metrics even if the synchronous flush timed out above.
for _ in $(seq 1 60); do
    result=$($CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.metric_log WHERE event_date >= yesterday() AND event_time >= now() - 600")
    if [[ "$result" == "1" ]]; then
        echo "1"
        exit 0
    fi
    sleep 2
done

# Final check — if we still have no data after 120s of polling, report the
# actual query result (likely "0") so the diff against the reference is clear.
$CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.metric_log WHERE event_date >= yesterday() AND event_time >= now() - 600"
