-- Verifies that when a part-level minmax index (over partition-key columns) already
-- proves a filter is true for an entire part, ClickHouse does not also evaluate the
-- granule-level minmax skip index on that part.

DROP TABLE IF EXISTS t_skip_granule_minmax;

CREATE TABLE t_skip_granule_minmax
(
    c Int32,
    INDEX i c TYPE minmax
)
ENGINE = MergeTree()
ORDER BY tuple()
PARTITION BY indexHint(c)
SETTINGS index_granularity = 1;

INSERT INTO t_skip_granule_minmax
SELECT 123 + number FROM numbers(1000);

-- Correctness: the count must match regardless of whether the optimization fires.
SELECT count() FROM t_skip_granule_minmax WHERE c > 100;
SELECT count() FROM t_skip_granule_minmax WHERE c > 500;
SELECT count() FROM t_skip_granule_minmax WHERE c > 10000;

-- Correctness with skip indexes disabled (for reference).
SELECT count() FROM t_skip_granule_minmax WHERE c > 100 SETTINGS use_skip_indexes = 0;

-- Run the covered-by-part-minmax query, then check that the trace log line was emitted.
SELECT count() FROM t_skip_granule_minmax WHERE c > 100
SETTINGS log_comment = '04113_skip_granule_minmax_fires';

SYSTEM FLUSH LOGS text_log, query_log;

SELECT count() > 0
FROM system.text_log
WHERE message LIKE '%Skipping granule-level evaluation of skip index%'
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE log_comment = '04113_skip_granule_minmax_fires'
        AND type = 'QueryFinish'
        AND current_database = currentDatabase()
  );

DROP TABLE t_skip_granule_minmax;
