-- PartialMergeJoin re-sorts left blocks by the join key, so read-in-order must not be
-- propagated through it. Otherwise Aggregating/Distinct-in-order consume a stream they
-- wrongly believe is sorted by the group/distinct key and mis-group rows. Issues #110662, #109216.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (k UInt32, n Nullable(Int64)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t2 (k UInt32, n Nullable(Int64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t1 SELECT number, if(number % 5 = 0, NULL, number) FROM numbers(1000);
INSERT INTO t2 SELECT number, if(number % 4 = 0, NULL, number) FROM numbers(500);

-- #110662: aggregation-in-order over a swapped partial_merge JOIN. 10 groups, count()=1 each.
SELECT r.k, count() FROM t1 AS l RIGHT JOIN t2 AS r ON l.n = r.n
WHERE r.k < 10 GROUP BY r.k ORDER BY r.k
SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 'true',
         optimize_aggregation_in_order = 1, max_threads = 1;

-- Same defect without the swap (plain LEFT join): must also be correct.
SELECT l.k, count() FROM t2 AS l LEFT JOIN t1 AS r ON l.n = r.n
WHERE l.k < 10 GROUP BY l.k ORDER BY l.k
SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 'false',
         optimize_aggregation_in_order = 1, max_threads = 1;

-- Plain LEFT join grouped by the left key, self-checking form: uniqExact(l.k) within each
-- GROUP BY l.k must be exactly 1 (any row mis-grouped into a wrong group makes it > 1).
DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
CREATE TABLE l (k Int32, j Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (j Int32) ENGINE = MergeTree ORDER BY j;
INSERT INTO l SELECT number % 50, number % 5 FROM numbers(4000);
INSERT INTO r SELECT number % 4 FROM numbers(3000);
SELECT max(u), min(u), count() FROM (
    SELECT l.k, uniqExact(l.k) AS u FROM l LEFT JOIN r ON l.j = r.j GROUP BY l.k
)
SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 'false',
         optimize_aggregation_in_order = 1, max_threads = 1;
DROP TABLE l;
DROP TABLE r;

-- #109216: distinct-in-order over a partial_merge JOIN. All 500 keys of t2 must survive.
SELECT count() FROM (
    SELECT DISTINCT l.k FROM t2 AS l LEFT JOIN t1 AS r ON l.n = r.n
    SETTINGS join_algorithm = 'partial_merge', optimize_distinct_in_order = 1, max_threads = 1
);

-- The left ReadFromMergeTree under a PartialMergeJoin must NOT read in order.
SET explain_query_plan_default = 'legacy';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.k, count() FROM t2 AS l LEFT JOIN t1 AS r ON l.n = r.n
    WHERE l.k < 10 GROUP BY l.k ORDER BY l.k
    SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 'false',
             optimize_aggregation_in_order = 1, max_threads = 1
) WHERE explain LIKE '%ReadType%';

DROP TABLE t1;
DROP TABLE t2;
