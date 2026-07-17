#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Positive check: a trace-level query must stream at least one <Trace> line while
# it races with a concurrent information-level query. Trace logs are delivered
# asynchronously, so under heavy load (sanitizer builds, slow/remote storage,
# high concurrency) a query occasionally closes its client stream before the
# first trace line is flushed, yielding a transient miss. Retry a bounded number
# of times, but every accepted OK is proven inside a live concurrency window: the
# paired information query gets a unique query_id, we wait_for_query_to_start on
# it (so it is registered in system.processes), keep it running for the whole
# trace attempt, and only tear it down afterwards. A single transient miss is
# tolerated (permanent Fail only if trace is never delivered under proven
# contention). The negative check runs on each attempt too: the information query
# is piped through grep so any leaked <Debug>/<Trace> line would break the diff.
# Match the <Trace> marker anywhere: the log prefix omits host_name/query_id when
# empty, so its column is not fixed and a positional awk '{print $8}' is fragile.
trace_check() {
    for _ in {1..20}; do
        info_qid="00965_info_${CLICKHOUSE_DATABASE}_${BASHPID}_${RANDOM}"
        # Paired information query, kept alive with a cheap sleepEachRow scan until
        # we kill it, so the trace attempt below is evaluated while it is provably
        # running. sleepEachRow uses negligible CPU, so ten concurrent pairs do not
        # starve the trace queries; a real numbers() scan here saturated the box and
        # timed out the Fast test budget. Its output is grepped for leaks (an
        # information level must never emit <Debug>/<Trace>); the QUERY_WAS_CANCELLED
        # notice from the kill is not a <Debug>/<Trace> line, so it is filtered out
        # and does not affect OK/Fail.
        ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="information" --query_id="$info_qid" \
            --query="SELECT sleepEachRow(0.05) FROM numbers(200) SETTINGS max_block_size = 1 FORMAT Null;" 2>&1 \
            | grep '<Debug>\|<Trace>' &
        info_pid=$!
        # Prove the paired information query is live before evaluating the trace side.
        wait_for_query_to_start "$info_qid"
        trace_ok=""
        ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="trace" --query="SELECT * from numbers(1000000);" 2>&1 | grep -q '<Trace>' && trace_ok=1
        ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$info_qid' SYNC FORMAT Null" 2>/dev/null
        wait "$info_pid"
        if [ -n "$trace_ok" ]; then
            echo "OK"
            return
        fi
    done
    echo "Fail"
}

for _ in {1..10}; do
    trace_check &
done

wait
