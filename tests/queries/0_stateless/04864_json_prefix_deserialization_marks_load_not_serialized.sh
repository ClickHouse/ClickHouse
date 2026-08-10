#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest
# no-random-settings: the test measures marks-load concurrency, which randomized read settings perturb.
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

cat > "$TD/config.xml" <<EOF
<clickhouse>
    <path>${TD}/data/</path>
    <tmp_path>${TD}/tmp/</tmp_path>
    <logger><level>none</level><console>false</console></logger>
    <!-- Size the limit from this process, not from the enclosing cgroup, which may already
         account for unrelated processes and would leave nothing for this instance. -->
    <max_server_memory_usage>8G</max_server_memory_usage>
    <memory_worker_use_cgroup>0</memory_worker_use_cgroup>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <engine>ENGINE = MergeTree PARTITION BY event_date ORDER BY event_time</engine>
    </query_log>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "$TD/config.xml" -q "
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

SELECT if(part_type = 'Wide', 'part is Wide', 'UNEXPECTED part type: ' || part_type)
FROM system.parts WHERE table = 't' AND active;

SELECT if(toTypeName(j.p0) = 'LowCardinality(String)', 'typed path is LowCardinality',
          'UNEXPECTED type: ' || toTypeName(j.p0))
FROM t LIMIT 1;

SYSTEM ENABLE FAILPOINT merge_tree_marks_load_sync_sleep;

-- Two arms: the second one additionally reaches the carriers that exist only while a marks load can
-- still be in flight.
SYSTEM DROP MARK CACHE;
SELECT count() FROM (SELECT j FROM t LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, log_comment = 'sync'
FORMAT Null;

SYSTEM DROP MARK CACHE;
SELECT count() FROM (SELECT j FROM t LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1, load_marks_asynchronously = 1,
         log_comment = 'async'
FORMAT Null;

SYSTEM DISABLE FAILPOINT merge_tree_marks_load_sync_sleep;
SYSTEM FLUSH LOGS query_log;

-- Load-bearing preconditions: without a measurable wait, or without a background load in the
-- asynchronous arm, the ratios below would be vacuous. The same query without the failpoint waits
-- under 15 ms, so one second is unreachable unless the injected sleep ran.
SELECT log_comment || ': ' || if(ProfileEvents['WaitMarksLoadMicroseconds'] > 1000000,
          'marks-load wait is measurable',
          'UNEXPECTED: no marks-load wait, failpoint did not fire')
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment IN ('sync', 'async') ORDER BY log_comment;

SELECT if(ProfileEvents['BackgroundLoadingMarksTasks'] > 0,
          'async arm scheduled background marks loads',
          'UNEXPECTED: async arm loaded every mark synchronously')
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = 'async';

-- The assertion. Master serializes every load behind one mutex, giving ratio ~= 1.0.
-- Concurrent loading pushes it well above 1; require a margin so the test is not timing-flaky.
SELECT log_comment || ': ' || if(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000) > 1.15,
          'marks loading overlaps across prefix tasks',
          'REGRESSION: marks loading is serialized, ratio = '
              || toString(round(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000), 2)))
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment IN ('sync', 'async') ORDER BY log_comment;
"
