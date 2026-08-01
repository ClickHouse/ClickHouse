#!/usr/bin/env bash
# Tags: long, no-parallel-replicas
# no-parallel-replicas: direct JOIN over a MergeTree right table is a single-node lookup.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_djs_left"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_djs_right"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_djs_left (id UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_djs_right (id UInt64, value String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_djs_right VALUES (1, 'a')"
# Seven probe keys (1..6 and 99), each in its own part (one INSERT each); only key 1 matches the
# right table, the rest take the not-found path. With `max_block_size=1` the direct-join lookup
# pipeline is rebuilt per left block, so several builds run over the shared `StorageSnapshot` and
# overlap, which is what the bug needs.
for k in 1 2 3 4 5 6; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_djs_left VALUES ($k)"
done
$CLICKHOUSE_CLIENT -q "INSERT INTO t_djs_left VALUES (99)"
# new_col is not physically present in the pre-ALTER right part; projecting it drives the
# mutations_snapshot path that reads the shared snapshot the strip optimization replaces.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_djs_right ADD COLUMN new_col String DEFAULT 'default_value'"

# The bug is on the direct-join path; confirm the exact STREAM query below actually plans through it
# (and fail loudly if planning changed and it silently stopped exercising the fixed code, e.g. if the
# STREAM query started to be rejected before the lookup pipeline is built).
$CLICKHOUSE_CLIENT --enable_streaming_queries=1 --enable_analyzer=1 --join_algorithm=direct -q "
    SELECT countIf(explain LIKE '%DirectKeyValueJoin%') > 0
    FROM (EXPLAIN PLAN SELECT l.id, r.value, r.new_col FROM t_djs_left AS l INNER JOIN t_djs_right AS r STREAM ON l.id = r.id)"

# Sanity-check the direct join itself: the bounded (non-STREAM) form must return the matching row
# with the ALTER-added column filled. Only key 1 matches.
$CLICKHOUSE_CLIENT --enable_analyzer=1 --join_algorithm=direct -q "
    SELECT l.id, r.value, r.new_col FROM t_djs_left AS l INNER JOIN t_djs_right AS r ON l.id = r.id ORDER BY l.id"

# Only the STREAM form takes the changed snapshot path: the planner disables parts-snapshot removal
# for a direct-join lookup (`PlannerJoinTree`, "not to remove parts snapshot"), and `ReadFromMergeTree`
# re-enables it only for `isStream`, so the streaming query below is what exercises the fix.
# `max_block_size=1` maximises the per-block direct-join pipeline rebuilds that create the overlap.
query="SELECT l.id, r.value, r.new_col FROM t_djs_left AS l INNER JOIN t_djs_right AS r STREAM ON l.id = r.id"
for _ in {1..25}; do
    # A STREAM read is continuous and never returns on its own. Run it in the background and only tear it
    # down once it has read at least one row, i.e. `read_rows > 0` in `system.processes`. That is the
    # proof the executor is attached and the MergeTree read is producing rows, i.e. it entered
    # `ReadFromMergeTree::initializePipeline` (the fixed path), rather than dying in the client
    # handshake/planning on a slow server. This is where the pre-fix binary aborts under MSan.
    qid="djs_${CLICKHOUSE_DATABASE}_$RANDOM"
    $CLICKHOUSE_CLIENT --enable_streaming_queries=1 --enable_analyzer=1 --join_algorithm=direct --max_block_size=1 --query_id="$qid" -q "$query" >/dev/null 2>&1 &
    q_pid=$!
    # Bounded wait for an execution-side signal (`read_rows > 0`), not mere `system.processes` visibility:
    # `ProcessList::insert` makes a query visible before the executor is attached, so visibility alone does
    # not prove the fixed read path ran. A probe that times out means the server is wedged or gone, so stop
    # and let the liveness checks below fail fast.
    reached=0
    q_start=$EPOCHSECONDS
    while (( EPOCHSECONDS - q_start <= 30 )); do
        rr=$(timeout 5 $CLICKHOUSE_CLIENT -q "SELECT max(read_rows) FROM system.processes WHERE query_id = '$qid'" 2>/dev/null) || break
        [ -n "$rr" ] && [ "$rr" -gt 0 ] 2>/dev/null && { reached=1; break; }
        sleep 0.1
    done
    # Deterministically stop the query now that it has run (bounded so a wedged live server cannot hang
    # here), then reap the background client.
    timeout 30 $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$qid' SYNC FORMAT Null" >/dev/null 2>&1
    kill "$q_pid" 2>/dev/null
    wait "$q_pid" 2>/dev/null
    # Stop early if the server went down (the pre-fix binary aborts under MSan).
    timeout 5 $CLICKHOUSE_CLIENT -q "SELECT 1" >/dev/null 2>&1 || break
    # Live server but the query never read a row within the wait window: this iteration proved nothing,
    # so fail loudly instead of falling through as a successful run.
    [ "$reached" = "1" ] || { echo "stream query did not start"; break; }
done

# Cleanup strictness is gated on server liveness, mirroring `clickhouse-test`'s own `_cleanup_database`
# ("best-effort only when the server is actually dead"): on a live server the KILL/drain/DROP steps are
# strict (a failure surfaces instead of being swallowed), so a leaked STREAM query or held table lock
# fails the test; only a confirmed-dead server downgrades to skip-and-fail-fast. Every step is still
# time-bounded, so even a wedged live server fails in seconds instead of hanging the job to its timeout
# (the pre-fix binary under Bugfix validation dies here and takes the dead branch).
if ! timeout 5 $CLICKHOUSE_CLIENT -q "SELECT 1" >/dev/null 2>&1; then
    # Server is dead (pre-fix binary aborted under MSan): the missing "alive" already fails the test,
    # and skipping cleanup avoids blocking on the unresponsive server.
    echo "server died"
else
    # Server is alive: cleanup must genuinely complete, and must fail FAST. The moment any bounded step
    # exceeds its timeout the outcome is already "cleanup incomplete", so stop right there instead of
    # running the remaining drain/DROP work (which could otherwise burn minutes on a wedged-but-live
    # server). A clean live server passes every step quickly and prints "alive"; any timeout skips the
    # rest and prints "cleanup incomplete", whose diff mismatch fails the test and surfaces the leak.
    cleanup() {
        # Stop any streaming query that outlived its client (else it holds the table locks and hangs
        # the DROP / lingers into the end-of-run hung check).
        timeout 60 $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id LIKE 'djs_${CLICKHOUSE_DATABASE}_%' SYNC FORMAT Null" >/dev/null 2>&1 || return 1
        # Confirm the process list drains; a probe that times out is a bounded step failing, so bail.
        local running
        for _ in {1..60}; do
            running=$(timeout 5 $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id LIKE 'djs_${CLICKHOUSE_DATABASE}_%'" 2>/dev/null) || return 1
            [ "$running" = "0" ] && break
            sleep 0.5
        done
        [ "$running" = "0" ] || return 1
        timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_djs_left" >/dev/null 2>&1 || return 1
        timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_djs_right" >/dev/null 2>&1 || return 1
    }
    if cleanup; then
        echo "alive"
    else
        echo "cleanup incomplete"
    fi
fi
