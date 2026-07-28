-- Regression test for a logical error (chassert) in KeyCondition::extractAtomFromTree:
-- a constant pointInPolygon polygon argument wrapped in Variant/Dynamic (e.g. if(c, arr, NULL))
-- has a non-Array const type, which tripped `chassert(WhichDataType(const_type).isArray())`
-- during primary-key analysis and aborted the server. The skip-index atom is now skipped for
-- such constants.
SET explain_query_plan_default = 'legacy';
DROP TABLE IF EXISTS t_pip_variant;
CREATE TABLE t_pip_variant (x Float64, y Float64) ENGINE = MergeTree ORDER BY (x, y);
INSERT INTO t_pip_variant VALUES (1, 1) (2, 2) (3, 3) (6, 4) (0, 0);

-- The polygon constant is wrapped in a Variant/Dynamic type (via if/multiIf/CAST), which used
-- to abort the server during key-condition analysis. Assert only that each query runs to
-- completion (no abort). The exact match count is intentionally not checked: it depends on
-- analyzer constant-folding (the old analyzer folds if()/multiIf() to a plain Array, the new
-- one keeps the Variant wrapper) and on a pre-existing pointInPolygon Variant-vs-Array
-- behavior, neither of which this regression targets.
SELECT count() >= 0 FROM t_pip_variant WHERE pointInPolygon((x, y), if(1, [(0., 0.), (8., 4.), (5., 8.)], NULL));
SELECT count() >= 0 FROM t_pip_variant WHERE pointInPolygon((x, y), if(0, NULL, [(0., 0.), (8., 4.), (5., 8.)]));
SELECT count() >= 0 FROM t_pip_variant WHERE pointInPolygon((x, y), multiIf(1, [(0., 0.), (8., 4.), (5., 8.)], NULL));
SELECT count() >= 0 FROM t_pip_variant WHERE pointInPolygon((x, y), CAST([(0., 0.), (8., 4.), (5., 8.)] AS Variant(Array(Tuple(Float64, Float64)), UInt8)));
SELECT count() >= 0 FROM t_pip_variant WHERE pointInPolygon((x, y), CAST([(0., 0.), (8., 4.), (5., 8.)] AS Dynamic));

-- Supertype path: if(cond, Array(Tuple), Tuple) folds the polygon constant to the common
-- supertype Variant(Array(Tuple), Tuple), again a non-Array const type that used to trip the
-- chassert during key-condition analysis. Building the plan (EXPLAIN) must run to completion;
-- whether the predicate later errors at execution is a separate function-level concern.
SELECT count() >= 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_pip_variant WHERE pointInPolygon((x, y), if(materialize(0), [(0., 0.), (8., 4.), (5., 8.)], (1., 2.))));

-- A plain Array constant must still take the skip-index analysis path: the key condition
-- carries the pointInPolygon atom. Asserted over EXPLAIN text so it is robust to plan-format
-- churn and analyzer differences.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_pip_variant WHERE pointInPolygon((x, y), [(0., 0.), (8., 4.), (5., 8.)])) WHERE explain ILIKE '%pointInPolygon%';

DROP TABLE t_pip_variant;
