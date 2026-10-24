-- Reproducer from https://github.com/ClickHouse/ClickHouse/issues/123363:
-- `use_skip_indexes = 0` must not disable the part-level min-max index on the partition-key columns.

SET enable_parallel_replicas = 0, use_query_condition_cache = 0;

DROP TABLE IF EXISTS t;

CREATE TABLE t (ts DateTime, k UInt64)
ENGINE = MergeTree PARTITION BY toStartOfDay(ts) ORDER BY k
SETTINGS auto_statistics_types = '';

SYSTEM STOP MERGES t;

-- 720 hourly rows = 30 daily parts; every part contains k = 0..9
INSERT INTO t SELECT toDateTime('2026-01-01 00:00:00') + number * 3600, number % 10 FROM numbers(720);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t WHERE k = 3 AND ts::Date IN (SELECT toDate('2026-01-05'))
    SETTINGS use_skip_indexes = 0
) WHERE explain LIKE '%Parts:%';

DROP TABLE t;
