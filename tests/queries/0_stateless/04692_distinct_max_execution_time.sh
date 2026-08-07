#!/usr/bin/env bash

# Test that DISTINCT honors max_execution_time inside a single transform() call in
# timeout_overflow_mode = 'break'.
#
# The pipeline executor only enforces max_execution_time between `work` calls, so a DISTINCT
# transform that does not check the soft timeout inside its inner loop would hash a huge single
# block to the end in one `transform` call and only stop when the executor notices the deadline
# afterwards. The soft-timeout latch in `DistinctTransform` checks the time limit every
# DEFAULT_BLOCK_SIZE rows and stops mid-block once the deadline passes.
#
# The data lives in a single 30M-row block, created on the INSERT side by setting
# max_block_size >= row count and max_threads = 1: the `numbers()` source then emits one 30M-row
# block which the INSERT pipeline's squashing transform (whose cap follows max_block_size) passes
# through unsplit, so the Memory table stores it as one block. A Memory table is used so the
# source is fast even on sanitizer builds; the read yields each stored block as-is, so the SELECT
# feeds DISTINCT a single 30M-row chunk in one `transform()` call. On slow builds (sanitizers)
# hashing the whole block without the latch takes ~60 s while the fixed version stops when the
# soft timeout fires (~5 s). The 20-second duration threshold below separates the two on such
# builds; on fast builds DISTINCT finishes the whole block before the deadline either way and
# stays well under the threshold, so the test only fails if the soft timeout is not enforced
# inside the transform.
#
# --interactive_delay=0 must be passed as a client option (not in the query SETTINGS clause):
# it is applied in TCPHandler before the query settings are parsed, and it makes the pull loop
# block so that neither the pull loop's checkTimeLimitSoft() polling (every interactive_delay ms)
# nor the CancellationChecker (a no-op in 'break' mode) can interrupt the transform mid-block.
# With the default 100 ms polling the buggy build is already stopped at the deadline and the test
# would not discriminate.
#
# Only the hash path (`DistinctTransform::buildFilter`) is tested here. The LowCardinality path
# (`buildLowCardinalityMask`) cannot be exercised by wall-clock: converting the data costs more
# than the deadline on sanitizer builds, so the query duration is dominated by the upstream
# expression regardless of the latch. It is covered deterministically by the integration test
# tests/integration/test_distinct_transform_kill_query/test.py::test_lc_soft_timeout.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique per-invocation log_comment suffix so the query_log lookup below sees only this run's rows,
# even when the test is re-run on a server whose query_log already holds rows from earlier attempts.
hash_comment="04692_distinct_timeout_hash_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t04692"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t04692 (n UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO t04692 SELECT number FROM numbers(30000000) SETTINGS max_rows_to_read = 0, max_block_size = 30000000, max_threads = 1"

$CLICKHOUSE_CLIENT --interactive_delay=0 --query "
    SELECT DISTINCT n FROM t04692
    SETTINGS max_block_size = 30000000, max_threads = 1,
        max_execution_time = 5, timeout_overflow_mode = 'break',
        max_rows_to_read = 0, log_comment = '${hash_comment}'
    FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
    SELECT if(query_duration_ms < 20000, 'OK', 'SLOW: ' || toString(query_duration_ms) || ' ms')
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND event_date >= yesterday()
        AND type = 'QueryFinish'
        AND log_comment = '${hash_comment}'
    SETTINGS max_rows_to_read = 0"
