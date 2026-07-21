-- Regression test for issue #104393: `WHERE p AND <LowCardinality(Nullable(int)) constant>`
-- returned zero rows on `MergeTree` because the part-pruning split in
-- `splitFilterNodeForAllowedInputs` reduced `(non_virtual AND const)` to
-- `notEquals(const, default)` using `removeNullable(result_type)`. For a
-- `LowCardinality(Nullable(X))` constant `removeNullable` left the type
-- unchanged, so the default became `NULL` (the dictionary type's default),
-- making the synthetic filter `notEquals(x, NULL)` -> `NULL` -> false, which
-- pruned every part before any data was read.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_104393;
CREATE TABLE t_104393 (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_104393 SELECT number FROM numbers(500);

-- Baseline.
SELECT count() FROM t_104393 WHERE (a > 0);

-- Each AND-with-truthy-constant variant must return the same 499 rows.
SELECT count() FROM t_104393 WHERE (a > 0) AND CAST(1 AS Nullable(UInt8));
SELECT count() FROM t_104393 WHERE (a > 0) AND CAST(1 AS LowCardinality(UInt8));
SELECT count() FROM t_104393 WHERE (a > 0) AND CAST(1 AS LowCardinality(Nullable(UInt8)));
SELECT count() FROM t_104393 WHERE (a > 0) AND CAST(1 AS LowCardinality(Nullable(UInt64)));
SELECT count() FROM t_104393 WHERE (a > 0) AND toLowCardinality(toNullable(toUInt8(1)));

-- A falsy `LowCardinality(Nullable(...))` constant must still prune to zero rows.
SELECT count() FROM t_104393 WHERE (a > 0) AND CAST(0 AS LowCardinality(Nullable(UInt8)));

-- A `NULL` constant must also produce zero rows (SQL three-valued logic).
SELECT count() FROM t_104393 WHERE (a > 0) AND CAST(NULL AS LowCardinality(Nullable(UInt8)));

-- Same predicate but on the PREWHERE path.
SELECT count() FROM t_104393 PREWHERE (a > 0) AND CAST(1 AS LowCardinality(Nullable(UInt8)));

-- Same predicate, prewhere disabled (the original report).
SELECT count() FROM t_104393 WHERE (a > 0) AND CAST(1 AS LowCardinality(Nullable(UInt8)))
SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

DROP TABLE t_104393;
