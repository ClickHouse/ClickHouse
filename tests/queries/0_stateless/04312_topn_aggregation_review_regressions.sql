-- Tags: no-random-settings, no-parallel-replicas

-- Regression tests for issues found during review of the TopNAggregation optimization:
--   1. Mode 1 (sorted-input early termination) must account for the MergeTree
--      sorting-key reverse flags, otherwise a descending sorting key reads the
--      wrong end of each group.
--   2. The rewrite must fall back to the standard pipeline when `max_rows_to_group_by`
--      is configured, because the TopN transforms cannot honor `group_by_overflow_mode`.
--   3. The rewrite must be disabled under `serialize_query_plan`, because
--      `TopNAggregatingStep` does not implement plan serialization.
-- `no-random-settings` keeps the EXPLAIN assertions stable.

DROP TABLE IF EXISTS t_topn_desc;
CREATE TABLE t_topn_desc (g UInt32, val UInt32)
ENGINE = MergeTree ORDER BY val DESC
SETTINGS allow_experimental_reverse_key = 1;

INSERT INTO t_topn_desc SELECT number % 100, number FROM numbers(10000);

-- (1) Descending sorting key: Mode 1 must be used AND results must be correct.
SELECT '-- DESC sorting key: TopNAggregating is used';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT g, max(val) AS m FROM t_topn_desc GROUP BY g ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

SELECT '-- DESC sorting key: Mode 1 (sorted input) is used';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT g, max(val) AS m FROM t_topn_desc GROUP BY g ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%Sorted input: true%';

SELECT '-- DESC sorting key: correct top-K (optimization on)';
SELECT g, max(val) AS m FROM t_topn_desc GROUP BY g ORDER BY m DESC LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- DESC sorting key: matches reference (optimization off)';
SELECT g, max(val) AS m FROM t_topn_desc GROUP BY g ORDER BY m DESC LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

DROP TABLE IF EXISTS t_topn_asc;
CREATE TABLE t_topn_asc (g UInt32, val UInt32) ENGINE = MergeTree ORDER BY val;
INSERT INTO t_topn_asc SELECT number % 100, number FROM numbers(10000);

-- (2) max_rows_to_group_by with throw: rewrite must be skipped and the standard
-- pipeline must enforce the limit (TOO_MANY_ROWS).
SELECT '-- max_rows_to_group_by: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT g, max(val) AS m FROM t_topn_asc GROUP BY g ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1, max_rows_to_group_by = 10, group_by_overflow_mode = 'throw'
) WHERE explain LIKE '%TopNAggregating%';

SELECT '-- max_rows_to_group_by: limit is enforced';
SELECT g, max(val) AS m FROM t_topn_asc GROUP BY g ORDER BY m DESC LIMIT 5
SETTINGS optimize_topn_aggregation = 1, max_rows_to_group_by = 10, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

-- (3) serialize_query_plan: rewrite must be skipped (no NOT_IMPLEMENTED) and produce correct results.
SELECT '-- serialize_query_plan: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT g, max(val) AS m FROM t_topn_asc GROUP BY g ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1, serialize_query_plan = 1
) WHERE explain LIKE '%TopNAggregating%';

SELECT '-- serialize_query_plan: correct top-K, no exception';
SELECT g, max(val) AS m FROM t_topn_asc GROUP BY g ORDER BY m DESC LIMIT 5
SETTINGS optimize_topn_aggregation = 1, serialize_query_plan = 1;

DROP TABLE t_topn_desc;
DROP TABLE t_topn_asc;
