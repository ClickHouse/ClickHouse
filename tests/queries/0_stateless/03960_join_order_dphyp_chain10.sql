-- Chain of 10 tables - at the default query_plan_optimize_join_order_limit boundary.
-- DPhyp: O(N^2) CSG-CP pairs for a chain; DPsize: O(3^N) = 59049 pairs.
-- Both must return the same result at the limit boundary.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

CREATE TABLE ch10_t1  (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t2  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t3  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t4  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t5  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t6  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t7  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t8  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t9  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t10 (id UInt32, prev_id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO ch10_t1  SELECT number, number FROM numbers(5);
INSERT INTO ch10_t2  SELECT number, number % 5 FROM numbers(10);
INSERT INTO ch10_t3  SELECT number, number % 10 FROM numbers(20);
INSERT INTO ch10_t4  SELECT number, number % 20 FROM numbers(30);
INSERT INTO ch10_t5  SELECT number, number % 30 FROM numbers(40);
INSERT INTO ch10_t6  SELECT number, number % 40 FROM numbers(50);
INSERT INTO ch10_t7  SELECT number, number % 50 FROM numbers(60);
INSERT INTO ch10_t8  SELECT number, number % 60 FROM numbers(70);
INSERT INTO ch10_t9  SELECT number, number % 70 FROM numbers(80);
INSERT INTO ch10_t10 SELECT number, number % 80, number * 3 FROM numbers(100);

SELECT sum(sipHash64(t1.val, t10.val))
FROM ch10_t1 t1
JOIN ch10_t2  t2  ON t1.id  = t2.prev_id
JOIN ch10_t3  t3  ON t2.id  = t3.prev_id
JOIN ch10_t4  t4  ON t3.id  = t4.prev_id
JOIN ch10_t5  t5  ON t4.id  = t5.prev_id
JOIN ch10_t6  t6  ON t5.id  = t6.prev_id
JOIN ch10_t7  t7  ON t6.id  = t7.prev_id
JOIN ch10_t8  t8  ON t7.id  = t8.prev_id
JOIN ch10_t9  t9  ON t8.id  = t9.prev_id
JOIN ch10_t10 t10 ON t9.id  = t10.prev_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPsize must produce the same result.
SELECT sum(sipHash64(t1.val, t10.val))
FROM ch10_t1 t1
JOIN ch10_t2  t2  ON t1.id  = t2.prev_id
JOIN ch10_t3  t3  ON t2.id  = t3.prev_id
JOIN ch10_t4  t4  ON t3.id  = t4.prev_id
JOIN ch10_t5  t5  ON t4.id  = t5.prev_id
JOIN ch10_t6  t6  ON t5.id  = t6.prev_id
JOIN ch10_t7  t7  ON t6.id  = t7.prev_id
JOIN ch10_t8  t8  ON t7.id  = t8.prev_id
JOIN ch10_t9  t9  ON t8.id  = t9.prev_id
JOIN ch10_t10 t10 ON t9.id  = t10.prev_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE ch10_t1;
DROP TABLE ch10_t2;
DROP TABLE ch10_t3;
DROP TABLE ch10_t4;
DROP TABLE ch10_t5;
DROP TABLE ch10_t6;
DROP TABLE ch10_t7;
DROP TABLE ch10_t8;
DROP TABLE ch10_t9;
DROP TABLE ch10_t10;
