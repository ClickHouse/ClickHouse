-- A relation whose cardinality cannot be estimated must not be costed as a single row: it would then
-- look like the cheapest thing to join, ahead of every relation the optimizer does have an estimate
-- for. `t_unknown` is filtered on a non-key column, so range analysis cannot estimate it, while
-- `t_big`, `t_small_a` and `t_small_b` are read whole and their row counts are known. The reference
-- pins the leaf order of the plan for the three solvers that cost plans. Costing `t_unknown` as one
-- row makes `t_small_a` x `t_unknown` the cheapest pair, which reorders all three plans.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;
SET explain_query_plan_default = 'legacy';
SET enable_join_transitive_predicates = 1;
-- The join order must follow from the relation row counts alone: no column statistics (they would
-- make the selectivity estimate reliable) and no measured row counts from an earlier execution.
SET use_statistics = 0;
SET use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS t_big;
DROP TABLE IF EXISTS t_small_a;
DROP TABLE IF EXISTS t_small_b;
DROP TABLE IF EXISTS t_unknown;

CREATE TABLE t_big (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE t_small_a (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE t_small_b (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE t_unknown (id UInt32, tag String) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';

INSERT INTO t_big SELECT number, number FROM numbers(100000);
INSERT INTO t_small_a SELECT number, number FROM numbers(10);
INSERT INTO t_small_b SELECT number, number FROM numbers(20);
INSERT INTO t_unknown SELECT number, 'x' FROM numbers(100);

SELECT 'greedy';
SELECT rel FROM (
    SELECT rowNumberInAllBlocks() AS n, extract(explain, 'ReadFromMergeTree \(.*\.(\w+)\)') AS rel
    FROM (
        EXPLAIN SELECT count() FROM t_big, t_small_a, t_small_b, t_unknown
        WHERE t_big.id = t_small_a.id AND t_small_a.id = t_small_b.id
          AND t_small_b.id = t_unknown.id AND t_unknown.tag = 'x'
        SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_parallel_replicas = 0
    )
) WHERE rel != '' ORDER BY n;

SELECT 'dpsub';
SELECT rel FROM (
    SELECT rowNumberInAllBlocks() AS n, extract(explain, 'ReadFromMergeTree \(.*\.(\w+)\)') AS rel
    FROM (
        EXPLAIN SELECT count() FROM t_big, t_small_a, t_small_b, t_unknown
        WHERE t_big.id = t_small_a.id AND t_small_a.id = t_small_b.id
          AND t_small_b.id = t_unknown.id AND t_unknown.tag = 'x'
        SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', enable_parallel_replicas = 0
    )
) WHERE rel != '' ORDER BY n;

SELECT 'dphyp';
SELECT rel FROM (
    SELECT rowNumberInAllBlocks() AS n, extract(explain, 'ReadFromMergeTree \(.*\.(\w+)\)') AS rel
    FROM (
        EXPLAIN SELECT count() FROM t_big, t_small_a, t_small_b, t_unknown
        WHERE t_big.id = t_small_a.id AND t_small_a.id = t_small_b.id
          AND t_small_b.id = t_unknown.id AND t_unknown.tag = 'x'
        SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0
    )
) WHERE rel != '' ORDER BY n;

-- A semi/anti join filters its preserved side, so charging it an unestimated relation's row count
-- defers a selective filter behind the larger join. Only DPsub with a conflict detector reorders them.
-- The expected order joins `t_big` after the filter and differs from the written order, so a plan that
-- keeps the query's own order fails these blocks as well.
SELECT 'dpsub semi, conflict detector';
SELECT rel FROM (
    SELECT rowNumberInAllBlocks() AS n, extract(explain, 'ReadFromMergeTree \(.*\.(\w+)\)') AS rel
    FROM (
        EXPLAIN SELECT count() FROM t_small_a
        SEMI LEFT JOIN t_unknown ON t_small_a.id = t_unknown.id AND t_unknown.tag = 'x'
        INNER JOIN t_big ON t_small_a.id = t_big.id
        INNER JOIN t_small_b ON t_small_a.id = t_small_b.id
        SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
                 query_plan_optimize_join_order_use_conflict_detector_a = 1, enable_parallel_replicas = 0
    )
) WHERE rel != '' ORDER BY n;

SELECT 'dpsub anti, conflict detector';
SELECT rel FROM (
    SELECT rowNumberInAllBlocks() AS n, extract(explain, 'ReadFromMergeTree \(.*\.(\w+)\)') AS rel
    FROM (
        EXPLAIN SELECT count() FROM t_small_a
        ANTI LEFT JOIN t_unknown ON t_small_a.id = t_unknown.id AND t_unknown.tag = 'x'
        INNER JOIN t_big ON t_small_a.id = t_big.id
        INNER JOIN t_small_b ON t_small_a.id = t_small_b.id
        SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
                 query_plan_optimize_join_order_use_conflict_detector_a = 1, enable_parallel_replicas = 0
    )
) WHERE rel != '' ORDER BY n;

SELECT 'semi result';
SELECT count() FROM t_small_a
SEMI LEFT JOIN t_unknown ON t_small_a.id = t_unknown.id AND t_unknown.tag = 'x'
INNER JOIN t_big ON t_small_a.id = t_big.id
INNER JOIN t_small_b ON t_small_a.id = t_small_b.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
         query_plan_optimize_join_order_use_conflict_detector_a = 1;

SELECT 'anti result';
SELECT count() FROM t_small_a
ANTI LEFT JOIN t_unknown ON t_small_a.id = t_unknown.id AND t_unknown.tag = 'x'
INNER JOIN t_big ON t_small_a.id = t_big.id
INNER JOIN t_small_b ON t_small_a.id = t_small_b.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
         query_plan_optimize_join_order_use_conflict_detector_a = 1;

SELECT 'result';
SELECT count() FROM t_big, t_small_a, t_small_b, t_unknown
WHERE t_big.id = t_small_a.id AND t_small_a.id = t_small_b.id
  AND t_small_b.id = t_unknown.id AND t_unknown.tag = 'x';

DROP TABLE t_big;
DROP TABLE t_small_a;
DROP TABLE t_small_b;
DROP TABLE t_unknown;
