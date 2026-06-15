-- Tags: no-random-settings, no-parallel-replicas

-- Regression test for a contract violation found during review of the TopNAggregation
-- optimization: `exact_rows_before_limit = 1` sets `always_read_till_end` on `LimitStep`,
-- which requires `LimitTransform` to keep consuming upstream data after producing `K` rows
-- (to report the full pre-limit row count and to preserve totals). The rewrite drops the
-- `LimitStep` and Mode 1 stops reading after `K` groups, so it cannot honor that contract
-- and must be rejected, like `limitPushDown` and `topKThroughJoin` do.
-- `no-random-settings` keeps the EXPLAIN assertions stable.

SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_topn_exact_rows;
CREATE TABLE t_topn_exact_rows (g UInt32, val UInt32) ENGINE = MergeTree ORDER BY val;
INSERT INTO t_topn_exact_rows SELECT number % 100, number FROM numbers(10000);

SELECT '-- exact_rows_before_limit = 1: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT g, max(val) AS m FROM t_topn_exact_rows GROUP BY g ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1, exact_rows_before_limit = 1
) WHERE explain LIKE '%TopNAggregating%';

SELECT '-- exact_rows_before_limit = 1: correct top-K';
SELECT g, max(val) AS m FROM t_topn_exact_rows GROUP BY g ORDER BY m DESC LIMIT 5
SETTINGS optimize_topn_aggregation = 1, exact_rows_before_limit = 1;

SELECT '-- exact_rows_before_limit = 0: has TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT g, max(val) AS m FROM t_topn_exact_rows GROUP BY g ORDER BY m DESC LIMIT 5
    SETTINGS optimize_topn_aggregation = 1, exact_rows_before_limit = 0
) WHERE explain LIKE '%TopNAggregating%';

DROP TABLE t_topn_exact_rows;
