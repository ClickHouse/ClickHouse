-- A comparison of a column with itself is decided for every row: the estimate must not treat it as an
-- unknown condition. The join order optimizer prints the estimated row count of each relation.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (k UInt32, x UInt32 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t2 (k UInt32, x UInt32 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY k;

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

INSERT INTO t1 SELECT number, number % 10 FROM numbers(1000);
INSERT INTO t2 SELECT number, number % 10 FROM numbers(100);

SET enable_analyzer = 1;
SET use_statistics = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_merge_filter_into_join_condition = 1;
SET explain_query_plan_default = 'legacy';

SELECT '-- no condition on t2';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM t1, t2 WHERE t1.k = t2.k) WHERE explain LIKE '%Join:%';

SELECT '-- always true: x = x, x <= x, x >= x';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM t1, t2 WHERE t1.k = t2.k AND t2.x = t2.x AND t2.x <= t2.x AND t2.x >= t2.x) WHERE explain LIKE '%Join:%';

SELECT '-- always false: x != x';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM t1, t2 WHERE t1.k = t2.k AND t2.x != t2.x) WHERE explain LIKE '%Join:%';

SELECT '-- always false: x < x';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM t1, t2 WHERE t1.k = t2.k AND t2.x < t2.x) WHERE explain LIKE '%Join:%';

SELECT '-- a real condition still counts';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM t1, t2 WHERE t1.k = t2.k AND t2.x = 3) WHERE explain LIKE '%Join:%';

DROP TABLE t1;
DROP TABLE t2;
