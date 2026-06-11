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

-- Part-level minmax index (built from the partition-key columns, no explicit secondary index).
-- A part mixing finite floats with NaN stored a finite [min, max] in the part-level minmax, so the
-- whole part was pruned for `NOT ((val >= 0) AND (val <= 3))` even though the NaN row satisfies it.
-- The part-level minmax must record the NaN like the skip index does. use_skip_indexes = 0 also
-- disables part-level minmax pruning, so it is the unindexed oracle.

DROP TABLE IF EXISTS t_106533_partlevel;

CREATE TABLE t_106533_partlevel (id UInt64, val Nullable(Float64))
ENGINE = MergeTree PARTITION BY isNull(val) ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_106533_partlevel VALUES (1, 1.0), (2, nan), (3, 3.0);

SELECT count() FROM t_106533_partlevel WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_106533_partlevel WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

-- The mixed part's stored max is now NaN, so for val > 500 checkInHyperrectangle keeps intersects = true
-- and reads the part conservatively. EXPLAIN confirms the part-level Min-Max does not prune it (no Parts: 0/1).
SELECT count() FROM t_106533_partlevel WHERE val > 500;
SELECT count() FROM t_106533_partlevel WHERE val > 500 SETTINGS use_skip_indexes = 0;
SELECT countIf(explain LIKE '%Parts: 0/1%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_106533_partlevel WHERE val > 500);

DROP TABLE t_106533_partlevel;

-- NaN-widening must not disable part-level pruning entirely: an all-finite part is still pruned for
-- a range no value satisfies. A separate single-part table keeps this merge-stable (the Min-Max prunes
-- the only part, so EXPLAIN shows Parts: 0/1).

DROP TABLE IF EXISTS t_106533_partlevel_finite;

CREATE TABLE t_106533_partlevel_finite (id UInt64, val Nullable(Float64))
ENGINE = MergeTree PARTITION BY isNull(val) ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_106533_partlevel_finite VALUES (1, 100.0), (2, 150.0), (3, 200.0);

SELECT count() FROM t_106533_partlevel_finite WHERE val > 500;
SELECT count() FROM t_106533_partlevel_finite WHERE val > 500 SETTINGS use_skip_indexes = 0;
SELECT countIf(explain LIKE '%Parts: 0/1%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_106533_partlevel_finite WHERE val > 500);

DROP TABLE t_106533_partlevel_finite;
