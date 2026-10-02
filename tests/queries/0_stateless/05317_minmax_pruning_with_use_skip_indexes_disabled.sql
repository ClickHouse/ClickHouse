-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/123363
-- `use_skip_indexes = 0` must not disable the part-level min-max index on partition-key columns.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_minmax_use_skip_indexes;

-- No statistics, so the `Statistics` step can not prune the parts in place of `Min-Max`.
CREATE TABLE t_minmax_use_skip_indexes (ts DateTime, k UInt64)
ENGINE = MergeTree PARTITION BY toStartOfDay(ts) ORDER BY k
SETTINGS auto_statistics_types = '', index_granularity = 8192;

SYSTEM STOP MERGES t_minmax_use_skip_indexes;

-- 30 daily parts, each one containing every `k`, so the primary key can not prune any part.
INSERT INTO t_minmax_use_skip_indexes SELECT toDateTime('2026-01-01 00:00:00') + number * 3600, number % 10 FROM numbers(720);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_minmax_use_skip_indexes WHERE k = 3 AND ts::Date IN (SELECT toDate('2026-01-05'))
) WHERE explain LIKE '%Min-Max%' OR explain LIKE '%Parts:%'
SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_minmax_use_skip_indexes WHERE k = 3 AND ts::Date IN (SELECT toDate('2026-01-05')) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_minmax_use_skip_indexes WHERE k = 3 AND ts::Date IN (SELECT toDate('2026-01-05')) SETTINGS use_skip_indexes = 1;

-- Same for the minmax_count projection.
SELECT count(), min(ts), max(ts) FROM t_minmax_use_skip_indexes WHERE toStartOfDay(ts) = '2026-01-05' SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_use_skip_indexes;

-- https://github.com/ClickHouse/ClickHouse/issues/115271: min-max pruning removes the `p = 1` part
-- before the partition pruner evaluates `intDiv(1, p - 1)` on it.
DROP TABLE IF EXISTS t_minmax_use_skip_indexes_div;

CREATE TABLE t_minmax_use_skip_indexes_div (p Int64, b UInt64)
ENGINE = MergeTree ORDER BY b PARTITION BY p
SETTINGS index_granularity = 1;

INSERT INTO t_minmax_use_skip_indexes_div VALUES (1, 10);

SELECT count() FROM t_minmax_use_skip_indexes_div WHERE p != 1 AND intDiv(1, p - 1) > 0;
SELECT count() FROM t_minmax_use_skip_indexes_div WHERE p != 1 AND intDiv(1, p - 1) > 0 SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_use_skip_indexes_div;
