-- A negated predicate leaf over a key that has both a space-filling curve and another key
-- expression over the same column. The leaf emits one atom per key expression, and the
-- space-filling-curve collapse rewrites a curve-argument range into a relaxed atom, so the
-- cleanup of relaxed atoms in negated multi-atom groups has to hold after that collapse too.

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

-- { echo }
EXPLAIN indexes = 1 SELECT count() FROM t_negated_curve_group WHERE NOT x;
EXPLAIN indexes = 1 SELECT count() FROM t_negated_curve_group WHERE x = 0;
EXPLAIN indexes = 1 SELECT count() FROM t_negated_curve_group WHERE NOT has([3, 5], x) SETTINGS optimize_rewrite_has_to_in = 0;
