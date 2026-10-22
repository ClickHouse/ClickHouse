-- dphyp,dpsize algorithm combination: DPhyp succeeds on a connected graph
-- so DPsize is never reached.  Result must match running dphyp alone.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

CREATE TABLE co_a (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE co_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE co_c (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO co_a SELECT number, number * 10 FROM numbers(10);
INSERT INTO co_b SELECT number, number % 10 FROM numbers(30);
INSERT INTO co_c SELECT number, number % 10 FROM numbers(50);

SELECT sum(sipHash64(a.val, b.id, c.id))
FROM co_a a
JOIN co_b b ON a.id = b.a_id
JOIN co_c c ON a.id = c.a_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,dpsize';

-- dphyp alone must give the same result.
SELECT sum(sipHash64(a.val, b.id, c.id))
FROM co_a a
JOIN co_b b ON a.id = b.a_id
JOIN co_c c ON a.id = c.a_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

DROP TABLE co_a;
DROP TABLE co_b;
DROP TABLE co_c;
