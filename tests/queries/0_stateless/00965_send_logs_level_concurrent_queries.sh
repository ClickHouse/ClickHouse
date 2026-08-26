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
# query that has finished streaming and is only waiting to be collected. The timeouts are
# what bound one probe: the defaults are minutes long, which no caller here can afford.
info_running() {
    [ "$(${CLICKHOUSE_CLIENT} --connect_timeout=3 --receive_timeout=5 -q "SELECT count() FROM system.processes WHERE query_id = '$1' AND is_all_data_sent = 0 SETTINGS use_query_cache = 0" 2>/dev/null)" = "1" ]
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
# The information query reads a named pipe held open by this shell, so it blocks until
# this shell closes it. That makes the window end when the trace verdict is in rather than
# when a timer expires, so a slow trace attempt cannot outlive the query it races.
trace_check() {
    local info_out info_qid info_pid trace_ok info_ready pipe hold_fd
    info_out=$(mktemp "${WORKDIR}/out_XXXXXX")
    pipe="${WORKDIR}/pipe_${BASHPID}"
    trap 'rm -f "$info_out" "$pipe"' RETURN
    for _ in {1..3}; do
        rm -f "$pipe"
        mkfifo "$pipe" || break
        # Opening a pipe read-write does not block, so this holds both ends while there is
        # still no reader: the query's own open then returns at once instead of parking until
        # a writer appears. That matters because an interrupted open is not restarted, so a
        # thread parked in one fails the query with CANNOT_OPEN_FILE, while a read is retried.
        # Let bash pick the descriptor, since fd 3 carries bash xtrace under CI.
        exec {hold_fd}<>"$pipe"
        printf 'x\n' >&"$hold_fd"
        info_qid="00965_info_${CLICKHOUSE_DATABASE}_${BASHPID}_${RANDOM}"
        # Closing this descriptor is what ends the window, so the client must not keep a copy
        # of it alive: end of file needs every write end gone, not just this one.
        ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="information" --query_id="$info_qid" \
            --query="SELECT count() FROM file('$pipe', 'TSV', 'a String') FORMAT Null;" \
            > "$info_out" 2>&1 {hold_fd}>&- &
        info_pid=$!
        trace_ok=""
        info_ready=""
        # The budget is spent in elapsed time, not in a number of probes, so a host on which
        # each probe is slow waits no longer. The byte written above is gone only once the
        # query has opened the pipe and read from it, so requiring that as well as
        # registration keeps the close below from preceding the query's own open.
        SECONDS=0
        while [ "$SECONDS" -lt 3 ]; do
            if ! read -t 0 -u "$hold_fd" && info_running "$info_qid"; then
                info_ready=1
                break
            fi
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
        if [ -n "$trace_ok" ]; then
            # Both halves are asserted over this window only. Closing the write end lets the
            # information query reach end of file and finish on its own, so its whole
            # lifetime is covered; its exit status is asserted too, since a client that fails
            # without printing would otherwise satisfy the negative check by emitting nothing.
            exec {hold_fd}>&-
            wait "$info_pid" || echo "information query failed"
            grep '<Debug>\|<Trace>' "$info_out"
            echo "OK"
            return
        fi
        # Nothing is asserted over an abandoned window, so end the attempt instead of
        # waiting out a client that may still be starting up. Unlinking before the write end
        # goes away leaves no instant at which the pipe is still reachable by name with no
        # writer left to release it, so a query that opens it late fails at once rather than
        # parking: by then the name is gone.
        kill "$info_pid" 2>/dev/null
        wait "$info_pid" 2>/dev/null
        rm -f "$pipe"
        exec {hold_fd}>&-
    done
    echo "Fail"
}

for _ in {1..10}; do
    trace_check &
done

wait
