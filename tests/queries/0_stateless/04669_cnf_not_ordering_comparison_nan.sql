-- https://github.com/ClickHouse/ClickHouse/issues/112036
-- With convert_query_to_cnf = 1, NOT (x < c) must not be folded into x >= c:
-- NaN fails every ordered comparison, so the two forms differ on NaN rows.

SET convert_query_to_cnf = 1;
SET enable_analyzer = 1;

SELECT 'negated ordering comparisons';
DROP TABLE IF EXISTS t_04669_cnf_nan;
CREATE TABLE t_04669_cnf_nan (x Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04669_cnf_nan VALUES (nan), (1), (100);
SELECT count() FROM t_04669_cnf_nan WHERE NOT (x < 65.5);
SELECT count() FROM t_04669_cnf_nan WHERE NOT (x > 65.5);
SELECT count() FROM t_04669_cnf_nan WHERE NOT (x <= 65.5);
SELECT count() FROM t_04669_cnf_nan WHERE NOT (x >= 65.5);
SELECT count() FROM t_04669_cnf_nan WHERE x >= 65.5;
SELECT count() FROM t_04669_cnf_nan WHERE x < 65.5;
SELECT count() FROM t_04669_cnf_nan WHERE NOT (x < 65.5) OR x < 0.5;

-- A comparison buried under another function must also keep NaN semantics.
SELECT 'function argument';
SELECT count() FROM t_04669_cnf_nan WHERE NOT (x + 0 < 65.5);

DROP TABLE t_04669_cnf_nan;

-- Results on an integer column stay correct (such atoms are still folded, see the
-- `folding shapes` section).
SELECT 'int column';
DROP TABLE IF EXISTS t_04669_cnf_int;
CREATE TABLE t_04669_cnf_int (i Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04669_cnf_int VALUES (1), (100);
SELECT count() FROM t_04669_cnf_int WHERE NOT (i < 65);
SELECT count() FROM t_04669_cnf_int WHERE NOT (i > 65);
DROP TABLE t_04669_cnf_int;

-- A comparison of a non-string operand with a string constant degenerates like NaN
-- when the string cannot be converted: <, <=, >, >= all return false, so such
-- comparisons must not be folded either.
SELECT 'string mixed';
DROP TABLE IF EXISTS t_04669_str;
CREATE TABLE t_04669_str (u UInt8, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04669_str VALUES (1, 'a');
SELECT count() FROM t_04669_str WHERE NOT (u < '257');
SELECT count() FROM t_04669_str WHERE NOT (e < 'zzz');
DROP TABLE t_04669_str;

-- Nullable(Float64) must not fold and must keep both NULL semantics and the NaN row.
SELECT 'nullable float';
DROP TABLE IF EXISTS t_04669_nullable;
CREATE TABLE t_04669_nullable (x Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04669_nullable VALUES (nan), (1), (100), (NULL);
SELECT count() FROM t_04669_nullable WHERE NOT (x < 65.5);
DROP TABLE t_04669_nullable;

-- The tautology elimination must not treat x >= c and x < c as complementary for a
-- Float column: both are false for NaN, so the disjunction is not always true.
SELECT 'tautology';
DROP TABLE IF EXISTS t_04669_taut;
CREATE TABLE t_04669_taut (x Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04669_taut VALUES (nan), (1), (100);
SET optimize_using_constraints = 1;
SELECT count() FROM t_04669_taut WHERE x >= 65.5 OR x < 65.5;
DROP TABLE t_04669_taut;

-- A constraint with a negated ordering comparison over a Float column stays a negated
-- atom and must be skipped by the comparison graph instead of being folded into the
-- opposite comparison.
SELECT 'constraint';
DROP TABLE IF EXISTS t_04669_constraint;
CREATE TABLE t_04669_constraint (x Float64, CONSTRAINT c1 ASSUME NOT (x < 0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04669_constraint VALUES (nan), (1), (100);
SET optimize_using_constraints = 1;
SELECT count() FROM t_04669_constraint WHERE NOT (x < 65.5);
DROP TABLE t_04669_constraint;

-- A negated ordering atom must not produce an index hint: the hint is derived from
-- the function name alone, so it would inherit the negation and prune the opposite
-- primary key range.
SELECT 'index hint';
DROP TABLE IF EXISTS t_04669_idxhint;
CREATE TABLE t_04669_idxhint (pk Int64, i Int64, CONSTRAINT c1 ASSUME pk < i) ENGINE = MergeTree ORDER BY pk;
INSERT INTO t_04669_idxhint VALUES (0, 10);
SET optimize_using_constraints = 1;
SET optimize_append_index = 1;
SELECT count() FROM t_04669_idxhint WHERE NOT (i < 5);
DROP TABLE t_04669_idxhint;

DROP TABLE IF EXISTS t_04669_idxhint_f;
CREATE TABLE t_04669_idxhint_f (pk Int64, x Float64, CONSTRAINT c1 ASSUME toFloat64(pk) < x) ENGINE = MergeTree ORDER BY pk;
INSERT INTO t_04669_idxhint_f VALUES (0, 10);
SELECT count() FROM t_04669_idxhint_f WHERE NOT (x < 5);
DROP TABLE t_04669_idxhint_f;

-- The full-match constraint elimination must not treat NOT (x < c) and x >= c as the
-- same atom for a Float column: a NaN row satisfies the constraint but not x >= 100.
SELECT 'constraint full match';
DROP TABLE IF EXISTS t_04669_fullmatch;
CREATE TABLE t_04669_fullmatch (x Float64, CONSTRAINT c1 ASSUME NOT (x < 100)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04669_fullmatch VALUES (nan), (200), (300);
SET optimize_using_constraints = 1;
SELECT count() FROM t_04669_fullmatch WHERE x >= 100;
SELECT count() FROM t_04669_fullmatch WHERE NOT (x < 100);
DROP TABLE t_04669_fullmatch;

-- Mixed foldable and non-foldable atoms in one WHERE keep their independent semantics.
SELECT 'mixed types';
DROP TABLE IF EXISTS t_04669_mixed;
CREATE TABLE t_04669_mixed (i Int64, f Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04669_mixed VALUES (1, nan), (100, 1);
SELECT count() FROM t_04669_mixed WHERE f < 65.5 OR NOT (i < 65);
SELECT count() FROM t_04669_mixed WHERE NOT (f < 65.5) OR i < 0;
DROP TABLE t_04669_mixed;

-- The fix also holds with the Float column in the primary key. Note: this pins the
-- CNF rewrite only; the primary key index analysis applies its own negation folding
-- independently of `convert_query_to_cnf` and is tracked separately.
SELECT 'float primary key';
DROP TABLE IF EXISTS t_04669_pk;
CREATE TABLE t_04669_pk (x Float64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04669_pk VALUES (nan), (1), (100);
SELECT count() FROM t_04669_pk WHERE NOT (x < 65.5);
DROP TABLE t_04669_pk;

-- Pin the type-aware folding shapes: Int and LowCardinality(String) comparisons fold
-- into the opposite comparison, a Float comparison keeps the explicit `not`.
SELECT 'folding shapes';
DROP TABLE IF EXISTS t_04669_shape;
CREATE TABLE t_04669_shape (i Int64, f Float64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_04669_shape WHERE NOT (i < 65)) WHERE explain LIKE '%function_name: greaterOrEquals%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_04669_shape WHERE NOT (i < 65)) WHERE explain LIKE '%function_name: not,%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_04669_shape WHERE NOT (f < 65.5)) WHERE explain LIKE '%function_name: not,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_04669_shape WHERE NOT (f < 65.5)) WHERE explain LIKE '%function_name: greaterOrEquals%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_04669_shape WHERE NOT (s < 'm')) WHERE explain LIKE '%function_name: greaterOrEquals%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_04669_shape WHERE NOT (s < 'm')) WHERE explain LIKE '%function_name: not,%';
DROP TABLE t_04669_shape;
