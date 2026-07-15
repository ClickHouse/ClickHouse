-- Over-limit partial optimization: N=11 tables, limit=10.
-- The optimizer sees at most 10 relations; the 11th table is added on top
-- of the optimized sub-plan without reordering.
-- Data is 1:1 in every join so count() = 5 regardless of join order.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

CREATE TABLE ol_t1  (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t2  (id UInt32, t1_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t3  (id UInt32, t2_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t4  (id UInt32, t3_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t5  (id UInt32, t4_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t6  (id UInt32, t5_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t7  (id UInt32, t6_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t8  (id UInt32, t7_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t9  (id UInt32, t8_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t10 (id UInt32, t9_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ol_t11 (id UInt32, t10_id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO ol_t1  SELECT number, number FROM numbers(5);
INSERT INTO ol_t2  SELECT number, number FROM numbers(5);
INSERT INTO ol_t3  SELECT number, number FROM numbers(5);
INSERT INTO ol_t4  SELECT number, number FROM numbers(5);
INSERT INTO ol_t5  SELECT number, number FROM numbers(5);
INSERT INTO ol_t6  SELECT number, number FROM numbers(5);
INSERT INTO ol_t7  SELECT number, number FROM numbers(5);
INSERT INTO ol_t8  SELECT number, number FROM numbers(5);
INSERT INTO ol_t9  SELECT number, number FROM numbers(5);
INSERT INTO ol_t10 SELECT number, number FROM numbers(5);
INSERT INTO ol_t11 SELECT number, number, number FROM numbers(5);

-- DPhyp with limit=10 on an 11-table chain: partial optimization, count = 5.
SELECT count()
FROM ol_t1 t1
JOIN ol_t2  t2  ON t1.id  = t2.t1_id
JOIN ol_t3  t3  ON t2.id  = t3.t2_id
JOIN ol_t4  t4  ON t3.id  = t4.t3_id
JOIN ol_t5  t5  ON t4.id  = t5.t4_id
JOIN ol_t6  t6  ON t5.id  = t6.t5_id
JOIN ol_t7  t7  ON t6.id  = t7.t6_id
JOIN ol_t8  t8  ON t7.id  = t8.t7_id
JOIN ol_t9  t9  ON t8.id  = t9.t8_id
JOIN ol_t10 t10 ON t9.id  = t10.t9_id
JOIN ol_t11 t11 ON t10.id = t11.t10_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp',
         query_plan_optimize_join_order_limit = 10;

-- DPsize with full limit=11 must give the same count.
SELECT count()
FROM ol_t1 t1
JOIN ol_t2  t2  ON t1.id  = t2.t1_id
JOIN ol_t3  t3  ON t2.id  = t3.t2_id
JOIN ol_t4  t4  ON t3.id  = t4.t3_id
JOIN ol_t5  t5  ON t4.id  = t5.t4_id
JOIN ol_t6  t6  ON t5.id  = t6.t5_id
JOIN ol_t7  t7  ON t6.id  = t7.t6_id
JOIN ol_t8  t8  ON t7.id  = t8.t7_id
JOIN ol_t9  t9  ON t8.id  = t9.t8_id
JOIN ol_t10 t10 ON t9.id  = t10.t9_id
JOIN ol_t11 t11 ON t10.id = t11.t10_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
         query_plan_optimize_join_order_limit = 11;

DROP TABLE ol_t1;
DROP TABLE ol_t2;
DROP TABLE ol_t3;
DROP TABLE ol_t4;
DROP TABLE ol_t5;
DROP TABLE ol_t6;
DROP TABLE ol_t7;
DROP TABLE ol_t8;
DROP TABLE ol_t9;
DROP TABLE ol_t10;
DROP TABLE ol_t11;
