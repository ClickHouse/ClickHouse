#!/usr/bin/env bash
# Tags: replica, zookeeper, no-fasttest, no-sanitizers-lsan, long
# Test that KILL QUERY works for ALTER DELETE with mutations_sync=1 on ReplicatedMergeTree.
# Ref: https://github.com/ClickHouse/ClickHouse/issues/97535

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="kill_query_mutation_sync_${CLICKHOUSE_DATABASE}_$RANDOM"
alter_stderr="${CLICKHOUSE_TMP}/kill_query_mutation_sync_${CLICKHOUSE_DATABASE}_$RANDOM.stderr"

alter_pid=""

cleanup()
{
    # Cancel the foreground ALTER first: if a pre-kill guard failed (wait_for_query_to_start timed out,
    # or the mutation never became in-flight) we exit before the KILL QUERY below, so kill it here and
    # reap the background client. Otherwise a fast "exit 1" leaves the ALTER running into later tests
    # (clickhouse-test only force-kills the test process group on timeout, and non-interactive bash does
    # not reap background jobs on exit).
    $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null 2>&1 || true
    [ -n "$alter_pid" ] && wait "$alter_pid" 2>/dev/null
    $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL MUTATION WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_kill_mutation'" >/dev/null 2>&1 || true
    $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_kill_mutation SYNC" >/dev/null 2>&1 || true
    rm -f "$alter_stderr"
}
trap cleanup EXIT

# One partition per id, so every part holds a single row. A DELETE mutation reads each part
# as one block, so sleepEachRow(2) sleeps 2s per part (below function_sleep_max_microseconds_per_block,
# the 3s default), while the whole mutation genuinely runs for ~60s across the 30 parts. This
# gives KILL QUERY a wide window to cancel a mutation that is actively executing, instead of one
# that would immediately throw TOO_SLOW on a single 100-row block.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_kill_mutation
    (
        id UInt64,
        value String
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_kill_mutation', '1')
    PARTITION BY id
    ORDER BY id
"

$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_kill_mutation SELECT number, toString(number) FROM numbers(30)"

$CLICKHOUSE_CLIENT --query_id="$query_id" --query "
    ALTER TABLE ${CLICKHOUSE_DATABASE}.t_kill_mutation DELETE WHERE sleepEachRow(2) = 1
    SETTINGS mutations_sync = 1, allow_nondeterministic_mutations = 1
" >/dev/null 2>"$alter_stderr" &
alter_pid=$!

wait_for_query_to_start "$query_id"

# wait_for_query_to_start only proves the foreground ALTER reached system.processes; the
# background mutation may still be queued at that point. Wait for an unambiguous "a part is
# actively mutating" signal (system.mutations.parts_in_progress_names non-empty for this table)
# so KILL QUERY lands in the middle of mutation execution, not while it is still queued.
in_progress=0
for _ in $(seq 1 600); do
    in_progress=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations
        WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_kill_mutation'
          AND is_done = 0 AND notEmpty(parts_in_progress_names)
        SETTINGS use_query_cache = 0")
    [[ "$in_progress" != "0" ]] && break
    sleep 0.1
done
if [[ "$in_progress" == "0" ]]; then
    echo "Mutation never entered an in-flight state (parts_in_progress_names stayed empty)" >&2
    exit 1
fi

# KILL QUERY must cancel the ALTER while the mutation is still running. The background ALTER
# client exits once the server returns the cancellation error.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Wait (bounded) for the background ALTER client to exit after the kill. On a build without
# the cancellation check in waitMutationToFinishOnReplicas, the ALTER would ignore the kill
# and stay blocked; the timeout turns that into an explicit failure instead of hanging for the
# whole test time limit.
for _ in $(seq 1 600); do
    kill -0 "$alter_pid" 2>/dev/null || break
    sleep 0.1
done
if kill -0 "$alter_pid" 2>/dev/null; then
    echo "ALTER still running 60s after KILL QUERY" >&2
    kill "$alter_pid" 2>/dev/null
fi
wait "$alter_pid" 2>/dev/null

# Assert the ALTER exited via cancellation, not by the mutation completing on its own:
# the client prints QUERY_WAS_CANCELLED only when KILL QUERY cancelled the running ALTER.
# Without this check the test is a false positive (wait_for_query_to_start only proves the
# query was observed once). clickhouse-test runs .sh tests without `set -e`, so the grep must
# fail the test explicitly rather than falling through to teardown with exit 0.
if grep -qF "QUERY_WAS_CANCELLED" "$alter_stderr"; then
    echo "QUERY_WAS_CANCELLED"
else
    echo "ALTER did not report QUERY_WAS_CANCELLED after KILL QUERY:" >&2
    cat "$alter_stderr" >&2
    exit 1
fi
