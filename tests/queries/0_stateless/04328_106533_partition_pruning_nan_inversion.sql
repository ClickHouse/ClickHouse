DROP TABLE IF EXISTS t_106533;

CREATE TABLE t_106533 (c0 Int32) ENGINE = MergeTree() ORDER BY c0 PARTITION BY c0;
INSERT INTO t_106533 VALUES (1), (-1);

-- The metamorphic pair from issue #106533: a single HAVING with the predicate inside an OR
-- (the predicate stays compound and is not pushed down) versus the same predicate split across
-- the branches of an INTERSECT (each branch's HAVING-NOT pushes down to partition pruning).
-- Before the fix, partition pruning rewrote `not(sqrt(c0) > 10)` into `sqrt(c0) <= 10`. With
-- `c0 = -1` the partition value `sqrt(-1)` is NaN, which is not in `(-Inf, 10]`, so the
-- partition was incorrectly pruned and the second query returned an empty result. The two
-- queries must agree.

SELECT c0 FROM t_106533 GROUP BY c0
HAVING NOT ((SUM(c0) > 0) OR (sqrt(c0) > 10))
SETTINGS aggregate_functions_null_for_empty = 1, enable_optimize_predicate_expression = 0;

SELECT c0 FROM t_106533 GROUP BY c0
HAVING NOT (SUM(c0) > 0)
INTERSECT DISTINCT
SELECT c0 FROM t_106533 GROUP BY c0
HAVING NOT (sqrt(c0) > 10)
SETTINGS aggregate_functions_null_for_empty = 1, enable_optimize_predicate_expression = 0;

-- Direct probe: the partition for c0 = -1 must NOT be pruned by `not(sqrt(c0) > 10)`.
-- IEEE-754 says `not(NaN > 10) = not(false) = true`, so both rows pass.

SELECT count() FROM t_106533 WHERE NOT (sqrt(c0) > 10);

DROP TABLE t_106533;

-- Float column: a non-constant Float operand can be NaN, so the inversion must be skipped.
-- Insert a NaN row alongside finite values; `not(f > 5)` must keep NaN, while `f <= 5` drops it.

DROP TABLE IF EXISTS t_106533_float;

CREATE TABLE t_106533_float (f Float64) ENGINE = MergeTree() ORDER BY f;
INSERT INTO t_106533_float VALUES (1.0), (2.5), (10.0), (cast(0/0 AS Float64));

SELECT count() FROM t_106533_float WHERE NOT (f > 5);
SELECT count() FROM t_106533_float WHERE f <= 5;

DROP TABLE t_106533_float;

-- Decimal cannot be NaN, so the inverse rewrite must still apply. Confirm via a count that
-- only differs from the unindexed scan if the index analysis is sound.

DROP TABLE IF EXISTS t_106533_dec;

CREATE TABLE t_106533_dec (d Decimal(10, 2)) ENGINE = MergeTree() ORDER BY d PARTITION BY d;
INSERT INTO t_106533_dec VALUES (1.0), (10.0), (100.0);

SELECT count() FROM t_106533_dec WHERE NOT (d > 5) SETTINGS optimize_use_projections = 0;

DROP TABLE t_106533_dec;
