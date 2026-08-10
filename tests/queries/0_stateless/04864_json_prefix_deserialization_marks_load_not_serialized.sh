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
mkdir -p "$TD/data" "$TD/tmp" "$TD/data2" "$TD/tmp2"
trap 'rm -rf "$TD"' EXIT

# $1 = subdirectory suffix, rest = extra top-level settings
write_config() {
    local suffix="$1"; shift
    {
        echo "<clickhouse>"
        echo "    <path>${TD}/data${suffix}/</path>"
        echo "    <tmp_path>${TD}/tmp${suffix}/</tmp_path>"
        echo "    <logger><level>none</level><console>false</console></logger>"
        # Size the limit from this process, not from the enclosing cgroup, which may already
        # account for unrelated processes and would leave nothing for this instance.
        echo "    <max_server_memory_usage>8G</max_server_memory_usage>"
        echo "    <memory_worker_use_cgroup>0</memory_worker_use_cgroup>"
        echo "    <memory_worker_dynamic_hard_limit>false</memory_worker_dynamic_hard_limit>"
        printf '    %s\n' "$@"
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
    } > "$TD/config$suffix.xml"
}

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

write_config ""

${CLICKHOUSE_LOCAL} --config-file "$TD/config.xml" -q "
$FIXTURE

SELECT if(part_type = 'Wide', 'part is Wide', 'UNEXPECTED part type: ' || part_type)
FROM system.parts WHERE table = 't' AND active;

SELECT if(toTypeName(j.p0) = 'LowCardinality(String)', 'typed path is LowCardinality',
          'UNEXPECTED type: ' || toTypeName(j.p0))
FROM t LIMIT 1;

SYSTEM ENABLE FAILPOINT merge_tree_marks_load_sync_sleep;

-- Three arms. 'async' additionally reaches the carriers that exist only while a marks load can
-- still be in flight; 'prefetch' additionally executes the prefix prefetch callback.
SYSTEM DROP MARK CACHE;
SELECT count() FROM (SELECT j FROM t LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, log_comment = 'sync'
FORMAT Null;

SYSTEM DROP MARK CACHE;
SELECT count() FROM (SELECT j FROM t LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, load_marks_asynchronously = 1,
         log_comment = 'async'
FORMAT Null;

SYSTEM DROP MARK CACHE;
SELECT count() FROM (SELECT j FROM t LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, load_marks_asynchronously = 1,
         local_filesystem_read_prefetch = 1, enable_filesystem_read_prefetches_log = 1,
         log_comment = 'prefetch'
FORMAT Null;

SYSTEM DISABLE FAILPOINT merge_tree_marks_load_sync_sleep;
SYSTEM FLUSH LOGS query_log, filesystem_read_prefetches_log;

-- Load-bearing preconditions: without a measurable wait, or without a background load in the
-- asynchronous arms, the ratios below would be vacuous. The same query without the failpoint waits
-- under 15 ms, so one second is unreachable unless the injected sleep ran.
SELECT log_comment || ': ' || if(ProfileEvents['WaitMarksLoadMicroseconds'] > 1000000,
          'marks-load wait is measurable',
          'UNEXPECTED: no marks-load wait, failpoint did not fire')
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment IN ('sync', 'async', 'prefetch') ORDER BY log_comment;

SELECT log_comment || ': ' || if(ProfileEvents['BackgroundLoadingMarksTasks'] > 0,
          'scheduled background marks loads',
          'UNEXPECTED: loaded every mark synchronously')
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment IN ('async', 'prefetch') ORDER BY log_comment;

-- One prefetch per dynamic path proves the prefix prefetch callback ran. Without
-- local_filesystem_read_prefetch the reader never enters it, so this is zero.
SELECT if(count() = 39, 'prefix prefetch callback ran for every dynamic path',
          'UNEXPECTED prefix prefetch count: ' || toString(count()))
FROM system.filesystem_read_prefetches_log WHERE path LIKE '%dynamic\_structure%';

-- The assertion. Master serializes every load behind one mutex, giving ratio ~= 1.0.
-- Concurrent loading pushes it well above 1; require a margin so the test is not timing-flaky.
SELECT log_comment || ': ' || if(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000) > 1.15,
          'marks loading overlaps across prefix tasks',
          'REGRESSION: marks loading is serialized, ratio = '
              || toString(round(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000), 2)))
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment IN ('sync', 'async', 'prefetch') ORDER BY log_comment;
"

# A separate instance whose marks-pool queue is smaller than the number of loads, so scheduling a
# load has to wait for a running one to finish. That wait must not happen while the reader's
# container mutex is held. ThreadPool raises queue_size to max_threads, so a queue below the pool
# size has no effect.
write_config "2" \
    "<load_marks_threadpool_queue_size>50</load_marks_threadpool_queue_size>"

${CLICKHOUSE_LOCAL} --config-file "$TD/config2.xml" -q "
$FIXTURE

SYSTEM ENABLE FAILPOINT merge_tree_marks_load_sync_sleep;
SYSTEM DROP MARK CACHE;
SELECT count() FROM (SELECT j FROM t LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, load_marks_asynchronously = 1,
         log_comment = 'queue'
FORMAT Null;
SYSTEM DISABLE FAILPOINT merge_tree_marks_load_sync_sleep;
SYSTEM FLUSH LOGS query_log;

-- More loads than the queue holds, so at least one schedule call had to wait.
SELECT if(ProfileEvents['BackgroundLoadingMarksTasks'] > 50,
          'queue arm scheduled more loads than the queue holds',
          'UNEXPECTED: only ' || toString(ProfileEvents['BackgroundLoadingMarksTasks']) || ' background loads')
FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = 'queue';

SELECT if(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000) > 1.15,
          'queue arm: marks loading overlaps across prefix tasks',
          'REGRESSION: marks loading is serialized under a full queue, ratio = '
              || toString(round(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000), 2)))
FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = 'queue';
"
