-- Null-safe equals (<=>): exercises JoinConditionOperator::NullSafeEquals
-- in computeSelectivity.
--
-- t1-t2 are connected by TWO predicates:
--   t1.id = t2.t1_id   (regular equijoin -> join edge for DPhyp connectivity)
--   t1.val <=> t2.val  (null-safe equals -> additional selectivity filter)
-- t2-t3 by one equijoin: t2.t3_id = t3.id
--
-- Data:  row 0: val 10  <=> 10   = true  
--        row 1: val NULL <=> NULL = true    (differs from plain =)
--        row 2: val 12  <=> 99   = false 
-- count() = 2

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

CREATE TABLE ns_t1 (id UInt32, val Nullable(UInt32)) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ns_t2 (id UInt32, t1_id UInt32, val Nullable(UInt32), t3_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ns_t3 (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO ns_t1 VALUES (0, 10), (1, NULL), (2, 12);
INSERT INTO ns_t2 VALUES (0, 0, 10, 0), (1, 1, NULL, 1), (2, 2, 99, 2);
INSERT INTO ns_t3 VALUES (0), (1), (2);

SELECT count()
FROM ns_t1 t1, ns_t2 t2, ns_t3 t3
WHERE t1.id = t2.t1_id
  AND (t1.val <=> t2.val)
  AND t2.t3_id = t3.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp',
         query_plan_optimize_join_order_limit = 3;

-- DPsize must agree.
SELECT count()
FROM ns_t1 t1, ns_t2 t2, ns_t3 t3
WHERE t1.id = t2.t1_id
  AND (t1.val <=> t2.val)
  AND t2.t3_id = t3.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
         query_plan_optimize_join_order_limit = 3;

DROP TABLE ns_t1;
DROP TABLE ns_t2;
DROP TABLE ns_t3;
