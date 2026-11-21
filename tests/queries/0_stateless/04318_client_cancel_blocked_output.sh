#!/usr/bin/env bash
# Tags: no-fasttest

# Pressing Ctrl+C in the client must terminate the output of a result set promptly,
# even when the client is blocked writing to a slow/stuck output sink.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_output.fifo"
PAGER_FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_output.pager.fifo"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_output.err"
OUTFILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_output.outfile"

CLIENT=""
HOLDER=""

# Release the per-case processes: the client, the optional FIFO reader, and a stuck pager.
reap_case()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    [ -n "$HOLDER" ] && kill "$HOLDER" 2>/dev/null
    # A stuck pager blocks on opening this FIFO and so never reads EOF on its stdin even after the
    # client is gone. Opening the FIFO read-write (which never blocks) gives it a writer to read
    # past, so it exits promptly instead of lingering in the stateless worker.
    [ -p "$PAGER_FIFO" ] && : <> "$PAGER_FIFO" 2>/dev/null
    wait 2>/dev/null
    CLIENT=""
    HOLDER=""
}

cleanup()
{
    reap_case
    rm -f "$FIFO" "$PAGER_FIFO" "$CLIENT_ERR" "$OUTFILE"
}
trap cleanup EXIT

# Run the client with a query whose result is effectively unbounded (so the server keeps the query
# running, blocked on the stuck sink) while the result is written to a sink that never drains, then
# send a single Ctrl+C and verify the client terminates. The infrastructure is shared between three
# code paths that write the result set to a different descriptor:
#   * a plain SELECT, which writes through the persistent `std_out`;
#   * SELECT ... INTO OUTFILE ... AND STDOUT, which writes through a separate `stdout_buf`;
#   * a SELECT with `--pager`, which writes through the pager's stdin pipe.
run_case()
{
    local label="$1"
    local query="$2"
    local use_pager="${3:-0}"

    local query_id="${CLICKHOUSE_DATABASE}_cancel_blocked_output_${label}"

    rm -f "$FIFO" "$PAGER_FIFO" "$OUTFILE"

    local -a pager_args=()
    local redirect="$FIFO"
    if [ "$use_pager" = "1" ]
    then
        # A pager that blocks on opening a FIFO with no writer never reads its stdin, so the client
        # fills the pager's stdin pipe and then blocks inside write(). The client's own stdout is
        # irrelevant here (the pager would have written there), so send it to /dev/null.
        mkfifo "$PAGER_FIFO"
        pager_args=(--pager "cat '$PAGER_FIFO' > /dev/null")
        redirect="/dev/null"
    else
        # A reader that opens the pipe but never consumes data: the client quickly fills the pipe
        # buffer and then blocks inside write(). Without an interruptible write, a single Ctrl+C
        # would only set the cancellation flag while the restarted write() keeps the client stuck.
        mkfifo "$FIFO"
        sleep 1000 < "$FIFO" &
        HOLDER=$!
    fi

    # Small blocks and disabled limits keep the test cheap and immune to the randomized settings
    # used by the flaky check, which could otherwise make the client error out early (e.g. a low
    # max_memory_usage) instead of reaching the blocked state we want to exercise.
    $CLICKHOUSE_CLIENT --query_id="$query_id" "${pager_args[@]}" \
        --query "$query
                 SETTINGS max_block_size = 8192, max_threads = 1, max_memory_usage = 0,
                          max_rows_to_read = 0, max_result_rows = 0, max_result_bytes = 0" \
        > "$redirect" 2> "$CLIENT_ERR" &
    CLIENT=$!

    # Wait until the query is actually running (and thus blocked writing the result).
    local started=0
    for _ in {0..120}
    do
        count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'" 2>/dev/null)
        if [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -ge 1 ]
        then
            started=1
            break
        fi
        # If the client has already exited, it never reached the blocked state - fail explicitly.
        kill -0 "$CLIENT" 2>/dev/null || break
        sleep 0.5
    done

    if [ "$started" -ne 1 ]
    then
        echo "FAIL ($label): the query did not reach the running state"
        echo "--- client stderr ---"
        cat "$CLIENT_ERR"
        reap_case
        return
    fi

    # A single Ctrl+C must be enough to terminate the client.
    kill -SIGINT "$CLIENT" 2>/dev/null

    # Give the client a few seconds to terminate.
    for _ in {0..50}
    do
        kill -0 "$CLIENT" 2>/dev/null || break
        sleep 0.2
    done

    if kill -0 "$CLIENT" 2>/dev/null
    then
        echo "FAIL ($label): client is still running after Ctrl+C"
    else
        echo "OK ($label): client terminated after Ctrl+C"
    fi

    reap_case
}

# The persistent `std_out` path.
run_case "select" \
    "SELECT number, repeat('x', 100) FROM numbers(1000000000)"

# The separate `stdout_buf` used by ForkWriteBuffer for `INTO OUTFILE ... AND STDOUT`. The stuck
# FIFO is the STDOUT sink here; the regular file part of the result simply goes to OUTFILE.
run_case "outfile_and_stdout" \
    "SELECT number, repeat('x', 100) FROM numbers(1000000000) INTO OUTFILE '$OUTFILE' AND STDOUT FORMAT TabSeparated"

# The pager's stdin pipe: a stuck pager that never reads its stdin must not defeat Ctrl+C either.
run_case "pager" \
    "SELECT number, repeat('x', 100) FROM numbers(1000000000)" \
    1
