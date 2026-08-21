-- Tags: no-random-settings, no-random-merge-tree-settings

SET use_variant_as_common_type = 0;

-- Case 1: scalar cross-type (if-branch, target nestable in Nullable). NULL -> DEFAULT '', 42 -> '42'.
DROP TABLE IF EXISTS t_cross_scalar;
CREATE TABLE t_cross_scalar (x Nullable(UInt8), y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cross_scalar VALUES (NULL, 'a'), (42, 'b');
ALTER TABLE t_cross_scalar MODIFY COLUMN x String DEFAULT '';
OPTIMIZE TABLE t_cross_scalar FINAL;
SELECT x, y FROM t_cross_scalar ORDER BY y;
DROP TABLE t_cross_scalar;

-- Case 2: LowCardinality target (if-branch). Exercises makeNullableOrLowCardinalityNullable:
-- the intermediate must be LowCardinality(Nullable(String)), and the result must materialize
-- as the non-nullable LowCardinality(String) (checked via toTypeName + OPTIMIZE FINAL).
DROP TABLE IF EXISTS t_cross_lc;
CREATE TABLE t_cross_lc (x Nullable(UInt8), y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cross_lc VALUES (NULL, 'a'), (7, 'b');
ALTER TABLE t_cross_lc MODIFY COLUMN x LowCardinality(String) DEFAULT 'def';
OPTIMIZE TABLE t_cross_lc FINAL;
SELECT toTypeName(x), x, y FROM t_cross_lc ORDER BY y;
DROP TABLE t_cross_lc;

-- Case 3: Array target (else-branch, type that cannot be nested in Nullable). NULL -> DEFAULT [],
-- '[1,2,3]' -> [1,2,3]. There is no valid Nullable(Array), so this drives the eager-safe path.
DROP TABLE IF EXISTS t_cross_array;
CREATE TABLE t_cross_array (x Nullable(String), y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cross_array VALUES (NULL, 'a'), ('[1,2,3]', 'b');
ALTER TABLE t_cross_array MODIFY COLUMN x Array(UInt8) DEFAULT [];
OPTIMIZE TABLE t_cross_array FINAL;
SELECT x, y FROM t_cross_array ORDER BY y;
DROP TABLE t_cross_array;

-- Case 4: Array target with short_circuit_function_evaluation = 'disable'. Proves the else-branch
-- does not depend on short-circuit: the NULL row must yield [] and must not parse a placeholder.
SET short_circuit_function_evaluation = 'disable';
DROP TABLE IF EXISTS t_cross_array_noshort;
CREATE TABLE t_cross_array_noshort (x Nullable(String), y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cross_array_noshort VALUES (NULL, 'a'), ('[1,2,3]', 'b');
ALTER TABLE t_cross_array_noshort MODIFY COLUMN x Array(UInt8) DEFAULT [];
OPTIMIZE TABLE t_cross_array_noshort FINAL;
SELECT x, y FROM t_cross_array_noshort ORDER BY y;
DROP TABLE t_cross_array_noshort;

-- Case 5: NOT NULL source, cross-type (if-branch). No NULL row ever reaches ifNull, so this
-- isolates the value-conversion half (1 -> '1', 2 -> '2') that the NULL-bearing cases test jointly.
DROP TABLE IF EXISTS t_cross_notnull;
CREATE TABLE t_cross_notnull (x Nullable(UInt8), y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cross_notnull VALUES (1, 'a'), (2, 'b');
ALTER TABLE t_cross_notnull MODIFY COLUMN x String DEFAULT '';
OPTIMIZE TABLE t_cross_notnull FINAL;
SELECT x, y FROM t_cross_notnull ORDER BY y;
DROP TABLE t_cross_notnull;

-- Case 6: Tuple target (else-branch, a non-Array type that cannot be nested in Nullable).
-- Confirms the else-branch generalizes beyond Array. NULL -> DEFAULT (0,0), '(1,2)' -> (1,2).
DROP TABLE IF EXISTS t_cross_tuple;
CREATE TABLE t_cross_tuple (x Nullable(String), y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cross_tuple VALUES (NULL, 'a'), ('(1,2)', 'b');
ALTER TABLE t_cross_tuple MODIFY COLUMN x Tuple(UInt8, UInt8) DEFAULT (0, 0);
OPTIMIZE TABLE t_cross_tuple FINAL;
SELECT x, y FROM t_cross_tuple ORDER BY y;
DROP TABLE t_cross_tuple;

-- Case 7: multi-part conversion. Three separate INSERTs make three parts with different null mixes;
-- the conversion runs per part and OPTIMIZE FINAL merges them. NULL -> 'd', 5 -> '5', NULL -> 'd'.
DROP TABLE IF EXISTS t_cross_multipart;
CREATE TABLE t_cross_multipart (x Nullable(UInt8), y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cross_multipart VALUES (NULL, 'a');
INSERT INTO t_cross_multipart VALUES (5, 'b');
INSERT INTO t_cross_multipart VALUES (NULL, 'c');
ALTER TABLE t_cross_multipart MODIFY COLUMN x String DEFAULT 'd';
OPTIMIZE TABLE t_cross_multipart FINAL;
SELECT x, y FROM t_cross_multipart ORDER BY y;
DROP TABLE t_cross_multipart;

-- Case 8: production default (use_variant_as_common_type = 1). The rest of the file uses 0 to
-- reproduce the bug; this confirms the fix is also correct under the setting users actually run.
SET use_variant_as_common_type = 1;
DROP TABLE IF EXISTS t_cross_variant;
CREATE TABLE t_cross_variant (x Nullable(UInt8), y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cross_variant VALUES (NULL, 'a'), (9, 'b');
ALTER TABLE t_cross_variant MODIFY COLUMN x String DEFAULT '';
OPTIMIZE TABLE t_cross_variant FINAL;
SELECT x, y FROM t_cross_variant ORDER BY y;
DROP TABLE t_cross_variant;

-- Case 9: else-branch with a target default not castable back to the source type (Map
-- target, Tuple source). Needs allow_experimental_nullable_tuple_type for the source.
SET use_variant_as_common_type = 0;
SET allow_experimental_nullable_tuple_type = 1;
DROP TABLE IF EXISTS t_cross_map_default;
CREATE TABLE t_cross_map_default (x Nullable(Tuple(Array(String), Array(UInt8))), y String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cross_map_default VALUES (([], []), 'a'), (NULL, 'b');
ALTER TABLE t_cross_map_default MODIFY COLUMN x Map(String, UInt8) DEFAULT map('a', 1);
OPTIMIZE TABLE t_cross_map_default FINAL;
SELECT x, y FROM t_cross_map_default ORDER BY y;
DROP TABLE t_cross_map_default;
