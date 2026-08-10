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

T="t_json_prefix_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS $T;
CREATE TABLE $T (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO $T
SELECT number,
       toJSONString(mapFromArrays(
           arrayMap(x -> concat('p', toString(x)), range(40)),
           arrayMap(x -> toString(x + number), range(40))))::JSON
FROM numbers(20000);
"

$CLICKHOUSE_CLIENT -q "SELECT if(part_type = 'Wide', 'part is Wide', 'UNEXPECTED part type: ' || part_type)
                       FROM system.parts WHERE database = currentDatabase() AND table = '$T' AND active"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT merge_tree_marks_load_sync_sleep"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP MARK CACHE"

QUERY_ID="04864_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "
SELECT count() FROM (SELECT j FROM $T LIMIT 1)
SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1
FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT merge_tree_marks_load_sync_sleep"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# A load-bearing precondition: if the sleep never fired, the ratio is meaningless.
$CLICKHOUSE_CLIENT -q "
SELECT if(ProfileEvents['WaitMarksLoadMicroseconds'] > 10000000,
          'marks-load wait is measurable',
          'UNEXPECTED: no marks-load wait, failpoint did not fire')
FROM system.query_log
WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'QueryFinish'"

# The assertion. Master serializes every load behind one mutex, giving ratio ~= 1.0.
# Concurrent loading pushes it well above 1; require a margin so the test is not timing-flaky.
$CLICKHOUSE_CLIENT -q "
SELECT if(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000) > 1.15,
          'marks loading overlaps across prefix tasks',
          'REGRESSION: marks loading is serialized, ratio = '
              || toString(round(ProfileEvents['WaitMarksLoadMicroseconds'] / (query_duration_ms * 1000), 2)))
FROM system.query_log
WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT -q "DROP TABLE $T"
