#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest
# no-random-settings: the assertions are timing ratios, so keep the runner from perturbing them.
# no-fasttest: needs a failpoint.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Parallel prefix deserialization of a JSON column must not serialize marks loading.
# Oracle: with a fixed sleep injected into every synchronous marks load, the total time spent
# waiting for marks (WaitMarksLoadMicroseconds) exceeds the query's wall-clock time only if
# several loads waited at the same time. A single mutex held across marks loading makes the two
# equal by construction, so ratio > 1 is a direct, timing-robust witness of real concurrency.
#
# clickhouse-local, because the failpoint is process-global and persistent: on a shared server a
# concurrent copy could clear it mid-measurement, and the ratio would then report a regression on a
# correct build.

TD="${CLICKHOUSE_TMP}/04864_${CLICKHOUSE_DATABASE}"
rm -rf "$TD"
mkdir -p "$TD/data" "$TD/tmp"
trap 'rm -rf "$TD"' EXIT

{
    echo "<clickhouse>"
    echo "    <path>${TD}/data/</path>"
    echo "    <tmp_path>${TD}/tmp/</tmp_path>"
    echo "    <logger><level>none</level><console>false</console></logger>"
    # Size the limit from this process, not from the enclosing cgroup, which may already
    # account for unrelated processes and would leave nothing for this instance.
    echo "    <max_server_memory_usage>8G</max_server_memory_usage>"
    echo "    <memory_worker_use_cgroup>0</memory_worker_use_cgroup>"
    echo "    <memory_worker_dynamic_hard_limit>false</memory_worker_dynamic_hard_limit>"
    # An always-empty mark cache: the object still exists, so the asynchronous loading paths
    # that dereference it work, but every lookup misses and every arm below loads marks cold.
    echo "    <mark_cache_size>0</mark_cache_size>"
    echo "    <query_log>"
    echo "        <database>system</database>"
    echo "        <table>query_log</table>"
    echo "        <engine>ENGINE = MergeTree PARTITION BY event_date ORDER BY event_time</engine>"
    echo "    </query_log>"
    echo "    <filesystem_read_prefetches_log>"
    echo "        <database>system</database>"
    echo "        <table>filesystem_read_prefetches_log</table>"
    echo "        <engine>ENGINE = MergeTree ORDER BY event_time</engine>"
    echo "    </filesystem_read_prefetches_log>"
    echo "</clickhouse>"
} > "$TD/config.xml"

FIXTURE="
-- p0 is typed LowCardinality so the part also carries a dictionary stream, which is what brings
-- has_uniform_marks_callback into the picture; the other 39 paths stay dynamic and feed the pool.
CREATE TABLE t (id UInt64, j JSON(p0 LowCardinality(String))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t
SELECT number,
       toJSONString(mapFromArrays(
           arrayMap(x -> concat('p', toString(x)), range(40)),
           arrayMap(x -> toString(x + number), range(40))))::JSON(p0 LowCardinality(String))
FROM numbers(20000);
"

MEASURED=$(${CLICKHOUSE_LOCAL} --config-file "$TD/config.xml" -q "
$FIXTURE

SELECT if(part_type = 'Wide', 'part is Wide', 'UNEXPECTED part type: ' || part_type)
FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;

SELECT if(toTypeName(j.p0) = 'LowCardinality(String)', 'typed path is LowCardinality',
          'UNEXPECTED type: ' || toTypeName(j.p0))
FROM t LIMIT 1;

SYSTEM ENABLE FAILPOINT merge_tree_marks_load_sync_sleep;

-- Three arms. 'async' additionally reaches the carriers that exist only while a marks load can
-- still be in flight; 'prefetch' additionally executes the prefix prefetch callback. Only 'async'
-- carries the overlap verdict at the end of this query; the other two run for the code paths they
-- reach and for the deterministic checks below.
SELECT count() FROM (SELECT j FROM t LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, log_comment = 'sync'
FORMAT Null;

SELECT count() FROM (SELECT j FROM t LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, load_marks_asynchronously = 1,
         log_comment = 'async'
FORMAT Null;

SELECT count() FROM (SELECT j FROM t LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, load_marks_asynchronously = 1,
         local_filesystem_read_prefetch = 1, enable_filesystem_read_prefetches_log = 1,
         log_comment = 'prefetch'
FORMAT Null;

SYSTEM DISABLE FAILPOINT merge_tree_marks_load_sync_sleep;
SYSTEM FLUSH LOGS query_log, filesystem_read_prefetches_log;

-- Load-bearing preconditions: without the injected sleep, or without a background load in the
-- asynchronous arms, the ratio below would be vacuous. Only 'sync' can attest the sleep: it loads
-- marks on the thread that waits for them, so its measured wait contains the sleep itself and a
-- co-scheduled machine can only lengthen it (24 s here against 10 ms with the failpoint off). An
-- asynchronous arm measures only what is left of a background load once its consumer reaches it, so
-- a starved consumer sees no wait even though every load slept. One attestation covers all three
-- arms, because the failpoint is process-global and stays enabled across them.
SELECT log_comment || ': ' || if(ProfileEvents['WaitMarksLoadMicroseconds'] > 1000000,
          'marks-load wait is measurable',
          'UNEXPECTED: no marks-load wait, failpoint did not fire')
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'sync';

SELECT log_comment || ': ' || if(ProfileEvents['BackgroundLoadingMarksTasks'] > 0,
          'scheduled background marks loads',
          'UNEXPECTED: loaded every mark synchronously')
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase()
      AND log_comment IN ('async', 'prefetch') ORDER BY log_comment;

-- One prefetch per dynamic path proves the prefix prefetch callback ran. Without
-- local_filesystem_read_prefetch the reader never enters it, so this is zero.
SELECT if(count() = 39, 'prefix prefetch callback ran for every dynamic path',
          'UNEXPECTED prefix prefetch count: ' || toString(count()))
FROM system.filesystem_read_prefetches_log WHERE path LIKE '%dynamic\_structure%';

-- The assertion. Master serializes every load behind one mutex, giving ratio <= 1.0.
-- Concurrent loading pushes it well above 1; require a margin so the test is not timing-flaky.
-- 'async' is the arm that carries it because it is the one whose wait is a wait on background
-- loads: a machine that slows the query down slows those loads too, so contention lands in the
-- numerator and the denominator together. In the other two arms each consumer waits for its own
-- load, while the denominator also carries data-stream reads (serial in 'sync', prefetched in
-- 'prefetch') that a loaded machine inflates on its own.
SELECT log_comment || ': ' || if(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000) > 1.15,
          'marks loading overlaps across prefix tasks',
          'REGRESSION: marks loading is serialized, ratio = '
              || toString(round(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000), 2)))
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'async';
")

# Overlap is a capability: one measurement above the threshold proves it, and a build that holds a
# lock across marks loading cannot produce one at any attempt, because non-overlapping waits cannot
# sum to more than the query they happened in. The flaky check runs a copy of this test on all but
# one of the runner's cores, though, so a co-scheduled machine can still push a single measurement
# under the threshold. Re-measure before reporting a regression, and do not replace this with a
# threshold that normalizes by a second measurement: contention landing on the baseline alone would
# then report a serialized build as concurrent, which turns a false alarm into a missed regression.
MAX_RETRIES=2
THRESHOLD_MILLI=1150

# A process that did not create $TD has no system.query_log, so a retry reads the process-wide
# counters instead. They describe the arm alone as long as it is the only other query in the
# process, which the SelectQuery count below asserts rather than assumes: SelectQuery is counted
# when a query starts, so the expected value is 2, the arm plus the reporting query itself.
# SelectQueryTimeMicroseconds is counted when a query finishes, so it holds the arm's duration and
# not this query's.
EXPECTED_SELECTS=2

retry_async_arm() {
    local verdict="$1" measurement selects bg_tasks ratio_milli

    for _retry in $(seq 1 "$MAX_RETRIES"); do
        # Command substitution, not process substitution: it waits for the process to exit, so the
        # next attempt cannot collide on the data directory's lock, and it does not close the pipe
        # early and cut the measurement off mid-write.
        measurement=$(${CLICKHOUSE_LOCAL} --config-file "$TD/config.xml" -q "
        SYSTEM ENABLE FAILPOINT merge_tree_marks_load_sync_sleep;

        SELECT count() FROM (SELECT j FROM t LIMIT 1)
        SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, load_marks_asynchronously = 1
        FORMAT Null;

        SYSTEM DISABLE FAILPOINT merge_tree_marks_load_sync_sleep;

        SELECT sumIf(value, event = 'SelectQuery'),
               sumIf(value, event = 'BackgroundLoadingMarksTasks'),
               toUInt64(round(1000 * sumIf(value, event = 'WaitMarksLoadMicroseconds')
                   / greatest(sumIf(value, event = 'SelectQueryTimeMicroseconds'), 1)))
        FROM system.events;
        ")
        read -r selects bg_tasks ratio_milli <<< "$measurement"

        # Same preconditions as the first measurement: the counters must describe this arm, and it
        # must have loaded marks in the background, or the ratio below would not mean anything. Both
        # are properties of a correct build rather than of the machine, unlike a floor on the wait
        # itself, which is only the part of a background load that outlives its consumer's arrival.
        [ "${selects:-0}" = "$EXPECTED_SELECTS" ] || break
        [ "${bg_tasks:-0}" -gt 0 ] || break

        if [ "${ratio_milli:-0}" -gt "$THRESHOLD_MILLI" ]; then
            echo "async: marks loading overlaps across prefix tasks"
            return
        fi
    done

    echo "$verdict"
}

while IFS= read -r line; do
    case "$line" in
        "async: REGRESSION: marks loading is serialized"*) retry_async_arm "$line" ;;
        *) echo "$line" ;;
    esac
done <<< "$MEASURED"
