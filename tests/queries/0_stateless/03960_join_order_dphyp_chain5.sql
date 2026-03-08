-- Chain of 5 tables: T1 - T2 - T3 - T4 - T5.
-- Exercises enumerateCmpRec to full depth: the complement of {T1} must grow
-- through three recursive levels to reach {T5}.
-- DPhyp and DPsize must return the same result hash.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

CREATE TABLE ch5_t1 (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch5_t2 (id UInt32, t1_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch5_t3 (id UInt32, t2_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch5_t4 (id UInt32, t3_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch5_t5 (id UInt32, t4_id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO ch5_t1 SELECT number, number * 10 FROM numbers(10);
INSERT INTO ch5_t2 SELECT number, number % 10 FROM numbers(50);
INSERT INTO ch5_t3 SELECT number, number % 50 FROM numbers(100);
INSERT INTO ch5_t4 SELECT number, number % 100 FROM numbers(200);
INSERT INTO ch5_t5 SELECT number, number % 200, number * 7 FROM numbers(500);

SELECT sum(sipHash64(t1.val, t5.val))
FROM ch5_t1 t1
JOIN ch5_t2 t2 ON t1.id = t2.t1_id
JOIN ch5_t3 t3 ON t2.id = t3.t2_id
JOIN ch5_t4 t4 ON t3.id = t4.t3_id
JOIN ch5_t5 t5 ON t4.id = t5.t4_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPsize must produce the same result.
SELECT sum(sipHash64(t1.val, t5.val))
FROM ch5_t1 t1
JOIN ch5_t2 t2 ON t1.id = t2.t1_id
JOIN ch5_t3 t3 ON t2.id = t3.t2_id
JOIN ch5_t4 t4 ON t3.id = t4.t3_id
JOIN ch5_t5 t5 ON t4.id = t5.t4_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE ch5_t1;
DROP TABLE ch5_t2;
DROP TABLE ch5_t3;
DROP TABLE ch5_t4;
DROP TABLE ch5_t5;
