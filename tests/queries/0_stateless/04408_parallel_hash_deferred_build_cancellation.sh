#!/usr/bin/env bash
# Cancellation / timeout of the deferred (exact-size) parallel_hash build replay.
# https://github.com/ClickHouse/ClickHouse/pull/108129#discussion_r3457832007
#
# A cold parallel_hash build with no size hint defers: it buffers the right-hand blocks and replays
# them in one `onBuildPhaseFinish` work item. That item must poll the query status itself, otherwise a
# large cold build reserves and replays to completion before `KILL QUERY` or an expired
# `max_execution_time` is observed. `onBuildPhaseFinish` calls `QueryStatus::checkTimeLimit()` before
# the reserve and between every buffered block; `checkTimeLimit` throws `TIMEOUT_EXCEEDED` for
# `max_execution_time` and `QUERY_WAS_CANCELLED` for a `KILL` (the same call), so both triggers exercise
# the same in-replay poll.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Force a cold, two-level, deferred parallel_hash build with no SpillingHashJoin wrapper:
#  - parallel_hash + parallel_hash_join_threshold=0 : always ConcurrentHashJoin
#  - collect_hash_table_stats_during_joins=0        : no size hint => the deferred (cold) build path
#  - max_bytes_*_before_external_join=0             : no spill wrapper, so the pure deferred build runs
#  - query_plan_join_swap_table='false'            : keep the large side as the build side
#  - enable_parallel_replicas=0                     : keep the whole build on one node
# The build (right) side has many distinct keys so the map goes two-level and the deferred reserve runs.
# The probe side is tiny on purpose: only the build/replay phase matters here. The settings are pinned
# on the query (not the session), so randomized session settings do not change the build behavior.
SETTINGS_BASE="join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 0, collect_hash_table_stats_during_joins = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_join_swap_table = 'false', enable_parallel_replicas = 0"
EVENT="HashJoinDeferredPreallocatedElementsInHashTables"

# --- max_execution_time must interrupt the build mid-flight, not run the whole build first ----------
# Machine independence: every threshold below is a fraction of the build duration D measured on the same
# machine, so it adapts to any CPU instead of using absolute milliseconds. 4 build threads keep each
# per-slot reserve small, so the limit is honored promptly once the replay starts. Run the unlimited
# build several times (the first is cold: jit / allocator / thread-pool warm-up) and take D as the
# *minimum* warm duration - a conservative steady-state, so the threshold is not inflated by a slow
# warm-up run and the timed trials below (which run at steady state) reliably exceed it without the fix.
to_query="SELECT count() FROM (SELECT number AS k FROM numbers(2)) AS l INNER JOIN (SELECT number AS k FROM numbers(8000000)) AS r USING (k) SETTINGS ${SETTINGS_BASE}, max_threads = 4"
warm_id="${CLICKHOUSE_DATABASE}_warm"
for w in 1 2 3 4; do
    $CLICKHOUSE_CLIENT --query_id="${warm_id}_${w}" --query "$to_query, max_execution_time = 0 FORMAT Null"
done
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
# Drop the first (cold) run; D = fastest of the warm runs. USED confirms the deferred reserve ran.
read -r D USED < <($CLICKHOUSE_CLIENT --query "
    SELECT min(query_duration_ms), toUInt8(max(ProfileEvents['$EVENT']) > 0)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND query_id IN ('${warm_id}_2', '${warm_id}_3', '${warm_id}_4')")
D=${D:-1000}
echo "deferred_build_used ${USED:-0}"

# With the in-replay poll a limit of 0.7*D stops the query at ~0.7*D. Without it, once the replay starts
# it runs to completion (~D) before the limit is noticed. So a stop before 0.82*D (between the limit and
# the full build) means the limit was honored mid-flight. Which phase the limit lands in (buffering vs
# replay) wobbles run to run, so this is probabilistic: run several trials and require a majority. A few
# trials whose limit happens to trip during the already-interruptible buffering phase still stop at
# ~0.7*D, so they never turn a working build into a failure; they only matter for the regression case,
# where the majority that reach the replay run to ~D and are not counted as early.
limit_ms=$(( D * 70 / 100 ))
[ "$limit_ms" -lt 30 ] && limit_ms=30
limit_s=$(awk "BEGIN { printf \"%.3f\", $limit_ms / 1000 }")
threshold_ms=$(( D * 82 / 100 ))
trials=7
majority=4
id_list=""
for trial in $(seq 1 "$trials"); do
    to_id="${CLICKHOUSE_DATABASE}_timeout_${trial}"
    id_list="${id_list}${id_list:+,}'${to_id}'"
    $CLICKHOUSE_CLIENT --query_id="$to_id" --query "$to_query, max_execution_time = $limit_s, timeout_overflow_mode = 'throw' FORMAT Null" >/dev/null 2>&1 || true
done
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
early=$($CLICKHOUSE_CLIENT --query "
    SELECT countIf(query_duration_ms < $threshold_ms)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing' AND query_id IN ($id_list)")
# A clear majority of the trials stopped before the full build => the limit was honored mid-flight.
[ "${early:-0}" -ge "$majority" ] && echo "max_execution_time_interrupts_build 1" || echo "max_execution_time_interrupts_build 0"

# --- KILL QUERY must cancel a cold deferred build --------------------------------------------------
# A single build thread stretches the build wall time (~0.5s) at modest memory, leaving a wide window
# to KILL the query while it is buffering/replaying. Retry a few times in case a run is fast enough to
# finish before the asynchronous KILL lands.
kill_query="SELECT count() FROM (SELECT number AS k FROM numbers(2)) AS l INNER JOIN (SELECT number AS k FROM numbers(6000000)) AS r USING (k) SETTINGS ${SETTINGS_BASE}, max_threads = 1, max_execution_time = 0"
kill_cancelled=0
for attempt in $(seq 1 5); do
    kill_id="${CLICKHOUSE_DATABASE}_kill_${attempt}"
    kill_err="${CLICKHOUSE_TMP}/04408_kill_${CLICKHOUSE_DATABASE}_${attempt}.err"
    $CLICKHOUSE_CLIENT --query_id="$kill_id" --query "$kill_query FORMAT Null" >/dev/null 2>"$kill_err" &
    bg=$!
    for _ in $(seq 1 2000); do
        [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$kill_id'")" = "1" ] && break
        kill -0 "$bg" 2>/dev/null || break
    done
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$kill_id' SYNC" >/dev/null
    wait "$bg" 2>/dev/null || true
    if grep -q -F "QUERY_WAS_CANCELLED" "$kill_err"; then
        kill_cancelled=1
        rm -f "$kill_err"
        break
    fi
    rm -f "$kill_err"
done
echo "kill_query_cancels_build $kill_cancelled"
