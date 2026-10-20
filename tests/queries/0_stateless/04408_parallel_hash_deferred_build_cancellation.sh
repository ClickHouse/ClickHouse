#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The timing thresholds come from repeated runs of the same query, so randomized settings would change what is measured.

# A deferred build inserts all stored blocks in the post-build phase. It must check `max_execution_time` and `KILL QUERY`
# between the blocks, as the streaming build does between `addBlockToJoin` calls.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A deferred build of the two-level layout. Only with a spill threshold do the stored blocks of an `INNER` join keep the
# keys, so it has one, far above the build. The left side is tiny: only the build counts.
SETTINGS_BASE="join_algorithm = 'parallel_hash', enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, collect_hash_table_stats_during_joins = 0, max_bytes_before_external_join = 10737418240, max_bytes_ratio_before_external_join = 0, query_plan_join_swap_table = 'false', enable_parallel_replicas = 0"
EVENT="HashJoinDeferredBuilds"

# `max_execution_time` must stop the build while the blocks are inserted, not after. Every threshold is a fraction of the
# build time D of the same query on the same machine, measured as the fastest of runs 2 to 4; run 1 warms the caches.
to_query="SELECT count() FROM (SELECT number AS k FROM numbers(2)) AS l INNER JOIN (SELECT number AS k FROM numbers(8000000)) AS r USING (k) SETTINGS ${SETTINGS_BASE}, max_threads = 4"
warm_id="${CLICKHOUSE_DATABASE}_warm"
for w in 1 2 3 4; do
    $CLICKHOUSE_CLIENT --query_id="${warm_id}_${w}" --query "$to_query, max_execution_time = 0 FORMAT Null"
done
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
read -r D USED < <($CLICKHOUSE_CLIENT --query "
    SELECT min(query_duration_ms), toUInt8(max(ProfileEvents['$EVENT']) > 0)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND query_id IN ('${warm_id}_2', '${warm_id}_3', '${warm_id}_4')")
D=${D:-1000}
echo "deferred_build_used ${USED:-0}"

# With a limit of D/2, a build that checks the limit while it inserts stops at about D/2, and one that does not at about D.
# Where the limit lands varies between runs, so a majority of 7 trials must stop before 0.85 * D. The wide band leaves
# room for the timing noise of sanitizer builds.
limit_ms=$(( D * 50 / 100 ))
[ "$limit_ms" -lt 30 ] && limit_ms=30
limit_s=$(awk "BEGIN { printf \"%.3f\", $limit_ms / 1000 }")
threshold_ms=$(( D * 85 / 100 ))
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
[ "${early:-0}" -ge "$majority" ] && echo "max_execution_time_interrupts_build 1" || echo "max_execution_time_interrupts_build 0"

# `KILL QUERY` must cancel the build. One build thread makes the build long enough to kill it; a run can still finish
# before the kill lands, so it is tried up to 8 times.
kill_query="SELECT count() FROM (SELECT number AS k FROM numbers(2)) AS l INNER JOIN (SELECT number AS k FROM numbers(6000000)) AS r USING (k) SETTINGS ${SETTINGS_BASE}, max_threads = 1, max_execution_time = 0"
kill_cancelled=0
for attempt in $(seq 1 8); do
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
