#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKDIR="${CLICKHOUSE_USER_FILES_UNIQUE}_00965"
rm -rf "${WORKDIR}"
mkdir -p "${WORKDIR}"
trap 'rm -rf "${WORKDIR}"' EXIT

# Match the <Trace> marker anywhere: the log prefix omits host_name/query_id when
# empty, so its column is not fixed and a positional awk '{print $8}' is fragile.

# True only on a definite positive answer, so a failed or empty probe counts as not
# running rather than silently satisfying a liveness check. is_all_data_sent excludes a
# query that has finished streaming and is only waiting to be collected.
info_running() {
    [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '$1' AND is_all_data_sent = 0 SETTINGS use_query_cache = 0" 2>/dev/null)" = "1" ]
}

# Two assertions, held over one shared window:
#   positive - a trace-level query streams at least one <Trace> line while an
#              information-level query runs alongside it;
#   negative - that information-level query emits no <Debug>/<Trace> line over its
#              whole lifetime. It is left to complete naturally because the log sites
#              reached only on completion (read statistics, peak memory usage) are
#              otherwise never covered.
# Trace logs are delivered asynchronously, so under heavy load (sanitizer builds, slow
# or remote storage, high concurrency) a query occasionally closes its client stream
# before the first trace line is flushed, yielding a transient miss. Retry a bounded
# number of times; a permanent Fail means trace was never delivered alongside a running
# information-level query.
#
# The information query reads a named pipe, so it blocks until this function writes to
# it. That makes the window end when the trace verdict is in rather than when a timer
# expires, so a slow trace attempt cannot outlive the query it is supposed to race. Its
# exit status is asserted as well, since a client that fails without printing would
# otherwise satisfy the negative check by emitting nothing.
trace_check() {
    local info_out info_qid info_pid trace_ok info_ready pipe release_pid
    info_out=$(mktemp "${WORKDIR}/out_XXXXXX")
    pipe="${WORKDIR}/pipe_${BASHPID}"
    trap 'rm -f "$info_out" "$pipe"' RETURN
    for _ in {1..3}; do
        rm -f "$pipe"
        mkfifo "$pipe" || break
        info_qid="00965_info_${CLICKHOUSE_DATABASE}_${BASHPID}_${RANDOM}"
        ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="information" --query_id="$info_qid" \
            --query="SELECT count() FROM file('$pipe', 'TSV', 'a String') FORMAT Null;" \
            > "$info_out" 2>&1 &
        info_pid=$!
        trace_ok=""
        info_ready=""
        # The information query blocks on the pipe until this function releases it, so it
        # cannot pass through the registered state while unobserved; the budget only has to
        # cover getting there. An exhausted budget costs one retry, not the whole script.
        for _ in {1..30}; do
            info_running "$info_qid" && { info_ready=1; break; }
            sleep 0.1
        done
        if [ -n "$info_ready" ]; then
            for _ in {1..20}; do
                ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="trace" --query="SELECT * from numbers(1000000);" 2>&1 | grep -q '<Trace>' && trace_ok=1
                [ -n "$trace_ok" ] && break
                info_running "$info_qid" || break
            done
            # Only accept while the information query is still on the server.
            info_running "$info_qid" || trace_ok=""
        fi
        # Release the pipe so the information query completes naturally. Opening the write
        # end blocks until a reader arrives, so it runs in the background: an information
        # client that never reached the read leaves no reader at all.
        { printf 'x\n' > "$pipe"; } 2>/dev/null &
        release_pid=$!
        wait "$info_pid" || echo "information query failed"
        kill "$release_pid" 2>/dev/null
        wait "$release_pid" 2>/dev/null
        grep '<Debug>\|<Trace>' "$info_out"
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
