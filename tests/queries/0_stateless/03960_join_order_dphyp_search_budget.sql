-- query_plan_optimize_join_order_max_searched_plans bounds the join order search
-- deterministically. On a dense graph that exceeds the budget, dphyp gives up and the next
-- algorithm in the chain produces the plan. Disabling the budget (0) lets dphyp finish.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET enable_parallel_replicas = 0;
SET cross_to_inner_join_rewrite = 0;
-- Low budget so a small clique already exceeds it.
SET query_plan_optimize_join_order_max_searched_plans = 10;

CREATE TABLE sb_a (k UInt32) ENGINE = MergeTree() PRIMARY KEY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE sb_b (k UInt32) ENGINE = MergeTree() PRIMARY KEY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE sb_c (k UInt32) ENGINE = MergeTree() PRIMARY KEY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE sb_d (k UInt32) ENGINE = MergeTree() PRIMARY KEY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE sb_e (k UInt32) ENGINE = MergeTree() PRIMARY KEY k SETTINGS auto_statistics_types = 'uniq';

INSERT INTO sb_a SELECT number FROM numbers(5);
INSERT INTO sb_b SELECT number FROM numbers(5);
INSERT INTO sb_c SELECT number FROM numbers(5);
INSERT INTO sb_d SELECT number FROM numbers(5);
INSERT INTO sb_e SELECT number FROM numbers(5);

-- Five-clique: dphyp enumerates more than 10 partial plans and exceeds the budget.
SELECT count()
FROM sb_a a, sb_b b, sb_c c, sb_d d, sb_e e
WHERE a.k = b.k AND a.k = c.k AND a.k = d.k AND a.k = e.k
  AND b.k = c.k AND b.k = d.k AND b.k = e.k
  AND c.k = d.k AND c.k = e.k AND d.k = e.k
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- dphyp,greedy falls back to greedy.
SELECT count()
FROM sb_a a, sb_b b, sb_c c, sb_d d, sb_e e
WHERE a.k = b.k AND a.k = c.k AND a.k = d.k AND a.k = e.k
  AND b.k = c.k AND b.k = d.k AND b.k = e.k
  AND c.k = d.k AND c.k = e.k AND d.k = e.k
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy';

-- dpsize is bounded by the same budget and also falls back.
SELECT count()
FROM sb_a a, sb_b b, sb_c c, sb_d d, sb_e e
WHERE a.k = b.k AND a.k = c.k AND a.k = d.k AND a.k = e.k
  AND b.k = c.k AND b.k = d.k AND b.k = e.k
  AND c.k = d.k AND c.k = e.k AND d.k = e.k
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize,greedy';

-- A disabled budget (0) lets dphyp complete on the same query.
SELECT count()
FROM sb_a a, sb_b b, sb_c c, sb_d d, sb_e e
WHERE a.k = b.k AND a.k = c.k AND a.k = d.k AND a.k = e.k
  AND b.k = c.k AND b.k = d.k AND b.k = e.k
  AND c.k = d.k AND c.k = e.k AND d.k = e.k
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', query_plan_optimize_join_order_max_searched_plans = 0;

DROP TABLE sb_a;
DROP TABLE sb_b;
DROP TABLE sb_c;
DROP TABLE sb_d;
DROP TABLE sb_e;
