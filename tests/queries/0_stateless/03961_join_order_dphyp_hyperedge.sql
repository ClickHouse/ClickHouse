-- Test: DPhyp multi-table hyperedge
--
-- The predicate  t1.a + t2.b + t3.c = t4.d + t5.e  is a binary equality whose
-- left operand references {T1, T2, T3} and whose right operand references {T4, T5}.
-- buildHyperedges therefore creates a real (non-degenerate) hyperedge with
--   left_rels  = {T1, T2, T3}
--   right_rels = {T4, T5}
--
-- The only valid CSG-CMP split that covers all five relations is
--   ({T1, T2, T3}) JOIN ({T4, T5})
-- because the predicate sources span exactly those two groups.  DPhyp discovers
-- this split by following hyperedge connectivity during CSG enumeration; DPsize
-- finds it by exhaustive search.  Both must produce identical results.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

CREATE TABLE he_t1 (id UInt32, a UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE he_t2 (id UInt32, t1_id UInt32, b UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE he_t3 (id UInt32, t2_id UInt32, c UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE he_t4 (id UInt32, d UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE he_t5 (id UInt32, t4_id UInt32, e UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

-- Left chain: T1 --(t1.id = t2.t1_id)--> T2 --(t2.id = t3.t2_id)--> T3
INSERT INTO he_t1 SELECT number, number      FROM numbers(10);
INSERT INTO he_t2 SELECT number, number % 10, number % 7 FROM numbers(20);
INSERT INTO he_t3 SELECT number, number % 20, number % 5 FROM numbers(40);

-- Right chain: T4 --(t4.id = t5.t4_id)--> T5
INSERT INTO he_t4 SELECT number, number      FROM numbers(10);
INSERT INTO he_t5 SELECT number, number % 10, number % 7 FROM numbers(20);

-- Show the DPhyp plan.  The join tree must reflect the bushy split
-- ((T1 JOIN T2 JOIN T3) JOIN (T4 JOIN T5)) imposed by the hyperedge.
SELECT 'DPhyp plan for multi-table hyperedge predicate';
EXPLAIN
SELECT t1.id, t5.id
FROM he_t1 t1, he_t2 t2, he_t3 t3, he_t4 t4, he_t5 t5
WHERE t1.id = t2.t1_id
  AND t2.id = t3.t2_id
  AND t4.id = t5.t4_id
  AND t1.a + t2.b + t3.c = t4.d + t5.e
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPhyp and DPsize must return the same result hash.
SELECT sum(sipHash64(t1.id, t2.id, t3.id, t4.id, t5.id))
FROM he_t1 t1, he_t2 t2, he_t3 t3, he_t4 t4, he_t5 t5
WHERE t1.id = t2.t1_id
  AND t2.id = t3.t2_id
  AND t4.id = t5.t4_id
  AND t1.a + t2.b + t3.c = t4.d + t5.e
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

SELECT sum(sipHash64(t1.id, t2.id, t3.id, t4.id, t5.id))
FROM he_t1 t1, he_t2 t2, he_t3 t3, he_t4 t4, he_t5 t5
WHERE t1.id = t2.t1_id
  AND t2.id = t3.t2_id
  AND t4.id = t5.t4_id
  AND t1.a + t2.b + t3.c = t4.d + t5.e
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE he_t1;
DROP TABLE he_t2;
DROP TABLE he_t3;
DROP TABLE he_t4;
DROP TABLE he_t5;
