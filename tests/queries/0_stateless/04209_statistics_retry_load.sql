-- Tags: no-parallel

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt64 STATISTICS(minmax), b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t SELECT number, number FROM numbers(1000);
INSERT INTO t SELECT number + 1000000, number FROM numbers(1000);

-- Enable failpoint: loadStatistics throws an exception
SYSTEM ENABLE FAILPOINT merge_tree_load_statistics_throw;

-- Query 1: exception is swallowed by filterPartsByStatistics, result must be correct
SELECT count() FROM t WHERE a > 500000
SETTINGS use_statistics_for_part_pruning = 1;

-- Disable failpoint
SYSTEM DISABLE FAILPOINT merge_tree_load_statistics_throw;

-- Query 2: before the fix, the poisoned empty cache would be hit (0 parts pruned),
-- after the fix both parts should participate in correct pruning.
SELECT count() FROM t WHERE a > 2000000
SETTINGS use_statistics_for_part_pruning = 1;

SYSTEM FLUSH LOGS query_log;

-- Key assertion: SelectedParts for query 2 should be 0
SELECT ProfileEvents['SelectedParts']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%FROM t WHERE a > 2000000%'
  AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1;

DROP TABLE t;
