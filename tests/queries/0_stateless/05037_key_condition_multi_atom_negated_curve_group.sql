-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- A negated predicate leaf over a key that has both a space-filling curve and another key
-- expression over the same column. The leaf emits one atom per key expression, and the
-- space-filling-curve collapse rewrites a curve-argument range into a relaxed atom, so the
-- cleanup of relaxed atoms in negated multi-atom groups has to hold after that collapse too.

SET explain_query_plan_default = 'legacy';
SET optimize_trivial_count_with_sparsity_filter = 0;

DROP TABLE IF EXISTS t_negated_curve_group;

CREATE TABLE t_negated_curve_group (x UInt32, y UInt32)
ENGINE = MergeTree ORDER BY (mortonEncode(x, y), plus(x, 1))
SETTINGS index_granularity = 4;

INSERT INTO t_negated_curve_group SELECT intDiv(number, 8), number % 8 FROM numbers(64);

-- Ground truth for the results below.
SELECT count() FROM t_negated_curve_group WHERE NOT x;
SELECT count() FROM t_negated_curve_group WHERE x = 0;
SELECT count() FROM t_negated_curve_group WHERE NOT has([3, 5], x) SETTINGS optimize_rewrite_has_to_in = 0;

-- Only the index-analysis lines are compared: the shape of the query plan around
-- `ReadFromMergeTree` differs between analyzers and with parallel replicas.
-- { echo }
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_negated_curve_group WHERE NOT x) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_negated_curve_group WHERE x = 0) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_negated_curve_group WHERE NOT has([3, 5], x) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
