-- Multiple predicates on the same table pair.
-- T1-T2 are linked by two equi-predicates (id and val).
-- computeSelectivity takes the minimum over all edges; both predicates must
-- be collected by getApplicableExpressions and passed to the cost model.
-- count() = 3 (all three rows satisfy both predicates simultaneously).

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

CREATE TABLE mp_t1 (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE mp_t2 (id UInt32, val UInt32, t3_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE mp_t3 (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO mp_t1 VALUES (0, 10), (1, 11), (2, 12);
INSERT INTO mp_t2 VALUES (0, 10, 0), (1, 11, 1), (2, 12, 2);
INSERT INTO mp_t3 VALUES (0), (1), (2);

SELECT count()
FROM mp_t1 t1, mp_t2 t2, mp_t3 t3
WHERE t1.id = t2.id
  AND t1.val = t2.val
  AND t2.t3_id = t3.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp',
         query_plan_optimize_join_order_limit = 3;

-- DPsize must agree.
SELECT count()
FROM mp_t1 t1, mp_t2 t2, mp_t3 t3
WHERE t1.id = t2.id
  AND t1.val = t2.val
  AND t2.t3_id = t3.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
         query_plan_optimize_join_order_limit = 3;

DROP TABLE mp_t1;
DROP TABLE mp_t2;
DROP TABLE mp_t3;
