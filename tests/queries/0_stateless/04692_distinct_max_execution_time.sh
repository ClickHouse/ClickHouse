#!/usr/bin/env bash

# Test that DISTINCT honors max_execution_time inside a single transform() call in
# timeout_overflow_mode = 'break'.
#
# The pipeline executor only enforces max_execution_time between `work` calls, so a DISTINCT
# transform that does not check the soft timeout inside its inner loop would hash a huge single
# block to the end in one `transform` call and only stop when the executor notices the deadline
# afterwards. The soft-timeout latch in `DistinctTransform` checks the time limit every
# DEFAULT_BLOCK_SIZE rows and stops mid-block once the deadline passes.#
# The data lives in a single 30M-row block (max_block_size >= row count, max_threads = 1). On
# slow builds (sanitizers) hashing the whole block without the latch takes ~60 s for the numeric
# path and ~200 s for the LowCardinality path, while the fixed version stops when the soft
# timeout fires (~5 s). The 20-second duration threshold below separates the two on such builds;
# on fast builds DISTINCT finishes the whole block before the deadline either way and stays well
# under the threshold, so the test only fails if the soft timeout is not enforced inside the
# transform.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t04692"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t04692 (n UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO t04692 SELECT number FROM numbers(30000000)"

# Numeric path: `DistinctTransform::buildFilter`.
$CLICKHOUSE_CLIENT --query "
    SELECT DISTINCT n FROM t04692
    SETTINGS max_block_size = 30000000, max_threads = 1,
        max_execution_time = 5, timeout_overflow_mode = 'break',
        log_comment = '04692_distinct_timeout_hash'
    FORMAT Null"

# LowCardinality path: `DistinctTransform::buildLowCardinalityMask`.
$CLICKHOUSE_CLIENT --query "
    SELECT DISTINCT CAST(toString(n) AS LowCardinality(String)) FROM t04692
    SETTINGS max_block_size = 30000000, max_threads = 1,
        max_execution_time = 5, timeout_overflow_mode = 'break',
        log_comment = '04692_distinct_timeout_lc'
    FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
    SELECT if(query_duration_ms < 20000, 'OK', 'SLOW: ' || toString(query_duration_ms) || ' ms')
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND event_date >= yesterday()
        AND type = 'QueryFinish'
        AND log_comment IN ('04692_distinct_timeout_hash', '04692_distinct_timeout_lc')
    ORDER BY log_comment"
