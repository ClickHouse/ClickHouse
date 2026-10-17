-- Tags: no-old-analyzer

-- Verifies `system.query_log.query_plan_hash`: equal for plans that differ only in their constants,
-- different when the plan differs, and the same value the plan document carries as `ShapeHash`.
--
-- Queries are told apart by `log_comment` rather than by a marker in the query text, because a
-- marker literal would itself be part of the plan being hashed. Each group of queries is named by
-- the prefix of its comments.

DROP TABLE IF EXISTS t_plan_hash;
DROP TABLE IF EXISTS t_plan_hash_right;

CREATE TABLE t_plan_hash (k UInt64, a UInt32, b UInt32, s String, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t_plan_hash SELECT number, number % 97, number % 13, toString(number % 7) FROM numbers(1000);

CREATE TABLE t_plan_hash_right (k UInt64, c UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_plan_hash_right SELECT number, number % 11 FROM numbers(500);

SET log_query_plan_hash = 1;

-- Same shape, different constants: one hash per group.

-- `5`, `500` and `100000` are a `UInt8`, a `UInt16` and a `UInt32`.
SELECT count() FROM t_plan_hash WHERE a > 5 SETTINGS log_comment = 'same_width_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE a > 500 SETTINGS log_comment = 'same_width_2' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE a > 100000 SETTINGS log_comment = 'same_width_3' FORMAT Null;

-- The sets have different contents and sizes.
SELECT count() FROM t_plan_hash WHERE a IN (1, 2, 3) SETTINGS log_comment = 'same_in_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE a IN (1, 2, 3, 4, 5, 6) SETTINGS log_comment = 'same_in_2' FORMAT Null;

SELECT count() FROM t_plan_hash WHERE s = 'x' SETTINGS log_comment = 'same_string_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE s = 'a longer string' SETTINGS log_comment = 'same_string_2' FORMAT Null;

-- Without a cap on the limit, both qualify for the same top-K optimizations. With one, a limit below
-- it gets a `__topKFilter` the other does not, which is a different plan.
SELECT a FROM t_plan_hash ORDER BY a LIMIT 3
SETTINGS query_plan_max_limit_for_top_k_optimization = 0, log_comment = 'same_limit_1' FORMAT Null;
SELECT a FROM t_plan_hash ORDER BY a LIMIT 30 OFFSET 5
SETTINGS query_plan_max_limit_for_top_k_optimization = 0, log_comment = 'same_limit_2' FORMAT Null;

-- Parameters of an aggregate function are literals.
SELECT quantile(0.5)(a) FROM t_plan_hash SETTINGS log_comment = 'same_parameter_1' FORMAT Null;
SELECT quantile(0.9)(a) FROM t_plan_hash SETTINGS log_comment = 'same_parameter_2' FORMAT Null;

-- The join choices are pinned: left to the optimizer they can legitimately differ between the two
-- queries, for example when the statistics cache has learned the size of the hash table from the
-- first one, and the hash would rightly report a different plan.
SELECT sum(c) FROM t_plan_hash AS l JOIN t_plan_hash_right AS r ON l.k = r.k WHERE r.c > 5
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false', log_comment = 'same_join_1' FORMAT Null;
SELECT sum(c) FROM t_plan_hash AS l JOIN t_plan_hash_right AS r ON l.k = r.k WHERE r.c > 7
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false', log_comment = 'same_join_2' FORMAT Null;

-- Different plans: two hashes per group.

SELECT count() FROM t_plan_hash WHERE a > 5 SETTINGS log_comment = 'different_column_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE b > 5 SETTINGS log_comment = 'different_column_2' FORMAT Null;

SELECT sum(a + 1) FROM t_plan_hash SETTINGS log_comment = 'different_function_1' FORMAT Null;
SELECT sum(a * 1) FROM t_plan_hash SETTINGS log_comment = 'different_function_2' FORMAT Null;

-- The same query, with the optimizer choosing differently.
SELECT k FROM t_plan_hash ORDER BY k LIMIT 3 SETTINGS optimize_read_in_order = 1, log_comment = 'different_read_in_order_1' FORMAT Null;
SELECT k FROM t_plan_hash ORDER BY k LIMIT 3 SETTINGS optimize_read_in_order = 0, log_comment = 'different_read_in_order_2' FORMAT Null;

SELECT count() FROM t_plan_hash WHERE b = 5 SETTINGS use_skip_indexes = 1, log_comment = 'different_skip_index_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE b = 5 SETTINGS use_skip_indexes = 0, log_comment = 'different_skip_index_2' FORMAT Null;

SELECT sum(c) FROM t_plan_hash AS l JOIN t_plan_hash_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false', log_comment = 'different_join_algorithm_1' FORMAT Null;
SELECT sum(c) FROM t_plan_hash AS l JOIN t_plan_hash_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 'false', log_comment = 'different_join_algorithm_2' FORMAT Null;

-- Capturing the plan does not change its shape, and fills the column on its own.
SELECT count() FROM t_plan_hash WHERE a > 5 GROUP BY b SETTINGS log_comment = 'profiled_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE a > 6 GROUP BY b
SETTINGS log_query_plan_hash = 0, log_query_plans = 1, log_comment = 'profiled_2' FORMAT Null;

SELECT count() FROM t_plan_hash WHERE a > 5 SETTINGS log_query_plan_hash = 0, log_comment = 'off' FORMAT Null;

SET log_query_plan_hash = 0;
SYSTEM FLUSH LOGS query_log;

SELECT
    replaceRegexpOne(log_comment, '_[0-9]+$', '') AS group_name,
    count() AS queries,
    uniqExact(query_plan_hash) AS hashes,
    countIf(query_plan_hash = 0) AS missing
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND match(log_comment, '^(same|different|profiled|off)')
GROUP BY group_name
ORDER BY group_name;

-- The `QueryStart` row is written after the plan was hashed, so it has the hash too.
SELECT 'query_start', uniqExact(query_plan_hash), countIf(query_plan_hash = 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'same_width_1';

-- Every step of a captured plan carries the hash of its subtree, and the root's is the column's.
WITH
    toJSONString(query_plan) AS plan,
    JSONExtractArrayRaw(plan, 'Nodes') AS nodes,
    arrayFirst(node -> JSONExtractString(node, 'Node Id') = JSONExtractString(plan, 'Root'), nodes) AS root
SELECT
    'shape_hash',
    length(nodes) > 1,
    arrayAll(node -> match(JSONExtractString(node, 'ShapeHash'), '^[0-9a-f]{16}$'), nodes),
    JSONExtractString(root, 'ShapeHash') = lower(leftPad(hex(query_plan_hash), 16, '0'))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'profiled_2';

DROP TABLE t_plan_hash;
DROP TABLE t_plan_hash_right;
