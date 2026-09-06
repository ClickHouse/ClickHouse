-- { echo }
-- The arrayExists -> has rewrite must not change the answer for a String/FixedString pair.
-- The rewrite is an analyzer pass, so the assertions below are only live with the analyzer on.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_fs;
CREATE TABLE t_fs (v Array(FixedString(3))) ENGINE = Memory;
INSERT INTO t_fs SELECT [toFixedString('V0', 3)];

-- Ground truth: equals compares the string family zero-padded.
SELECT toFixedString('V0', 3) = 'V0\0', toFixedString('V0', 3) = 'V0\0\0';

-- A needle as wide as the element, and wider, with an all-NUL tail.
SELECT arrayExists(x -> x = 'V0\0', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = 'V0\0', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = 'V0\0\0', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = 'V0\0\0', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- The constant may sit on either side of the comparison.
SELECT arrayExists(x -> 'V0\0' = x, v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> 'V0\0' = x, v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- A tail that is not all NUL must still not match.
SELECT arrayExists(x -> x = 'V0abc', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = 'V0abc', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- A needle narrower than the element already agreed.
SELECT arrayExists(x -> x = 'V0', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = 'V0', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- Cross-width FixedString over a constant array, where has() compares raw Fields.
SELECT arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = toFixedString('V0', 4), v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = toFixedString('V0', 4), v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- A FixedString needle against a String element diverges in the same way.
DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (v Array(String)) ENGINE = Memory;
INSERT INTO t_str SELECT ['V0'];
SELECT arrayExists(x -> x = toFixedString('V0', 3), v) FROM t_str SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = toFixedString('V0', 3), v) FROM t_str SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- LowCardinality: through the rewrite this used to throw TOO_LARGE_STRING_SIZE.
DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (v Array(LowCardinality(FixedString(3)))) ENGINE = Memory;
INSERT INTO t_lc SELECT [toFixedString('V0', 3)];
SELECT arrayExists(x -> x = 'V0\0\0', v) FROM t_lc SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = 'V0\0\0', v) FROM t_lc SETTINGS optimize_rewrite_array_exists_to_has = 1;

DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (v Array(Nullable(FixedString(3)))) ENGINE = Memory;
INSERT INTO t_null SELECT [toFixedString('V0', 3)];
SELECT arrayExists(x -> x = 'V0\0\0', v) FROM t_null SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = 'V0\0\0', v) FROM t_null SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- Tuple elements are compared element-wise, at any nesting depth.
DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_tuple (v Array(Tuple(FixedString(3)))) ENGINE = Memory;
INSERT INTO t_tuple SELECT [tuple(toFixedString('V0', 3))];
SELECT arrayExists(x -> x = tuple('V0\0'), v) FROM t_tuple SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = tuple('V0\0'), v) FROM t_tuple SETTINGS optimize_rewrite_array_exists_to_has = 1;

DROP TABLE IF EXISTS t_tuple2;
CREATE TABLE t_tuple2 (v Array(Tuple(Tuple(FixedString(3))))) ENGINE = Memory;
INSERT INTO t_tuple2 SELECT [tuple(tuple(toFixedString('V0', 3)))];
SELECT arrayExists(x -> x = tuple(tuple('V0\0')), v) FROM t_tuple2 SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = tuple(tuple('V0\0')), v) FROM t_tuple2 SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = tuple('V0\0'), [tuple(toFixedString('V0', 3))]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = tuple('V0\0'), [tuple(toFixedString('V0', 3))]) SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- Array and Map elements: over a column both spellings answer 0, but over a constant array
-- `has` compares raw Fields and answers 1 where `equals` answers 0, so they diverge as well.
DROP TABLE IF EXISTS t_nested_arr;
CREATE TABLE t_nested_arr (v Array(Array(FixedString(3)))) ENGINE = Memory;
INSERT INTO t_nested_arr SELECT [[toFixedString('V0', 3)]];
SELECT arrayExists(x -> x = ['V0\0'], v) FROM t_nested_arr SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = ['V0\0'], v) FROM t_nested_arr SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = ['V0\0'], [[toFixedString('V0', 3)]]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = ['V0\0'], [[toFixedString('V0', 3)]]) SETTINGS optimize_rewrite_array_exists_to_has = 1;

DROP TABLE IF EXISTS t_nested_map;
CREATE TABLE t_nested_map (v Array(Map(String, FixedString(3)))) ENGINE = Memory;
INSERT INTO t_nested_map SELECT [map('k', toFixedString('V0', 3))];
SELECT arrayExists(x -> x = map('k', 'V0\0'), v) FROM t_nested_map SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = map('k', 'V0\0'), v) FROM t_nested_map SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = map('k', 'V0\0'), [map('k', toFixedString('V0', 3))]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = map('k', 'V0\0'), [map('k', toFixedString('V0', 3))]) SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- The key side of a Map is compared too.
SELECT arrayExists(x -> x = map('k\0', 'V'), [map(toFixedString('k', 2), 'V')]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = map('k\0', 'V'), [map(toFixedString('k', 2), 'V')]) SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- Mixed nesting: the pair is reached through a tuple and then an array.
SELECT arrayExists(x -> x = tuple(['V0\0']), [tuple([toFixedString('V0', 3)])]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = tuple(['V0\0']), [tuple([toFixedString('V0', 3)])]) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = tuple(['V0\0']), v) FROM (SELECT [tuple([toFixedString('V0', 3)])] AS v) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = tuple(['V0\0']), v) FROM (SELECT [tuple([toFixedString('V0', 3)])] AS v) SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- A container whose members have identical types keeps the rewrite, and so does a numeric one.
SELECT arrayExists(x -> x = [toFixedString('V0', 3)], [[toFixedString('V0', 3)]]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = [toFixedString('V0', 3)], [[toFixedString('V0', 3)]]) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = map('k', toFixedString('V0', 3)), [map('k', toFixedString('V0', 3))]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = map('k', toFixedString('V0', 3)), [map('k', toFixedString('V0', 3))]) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = [1, 2], [[1, 2], [3]]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = [1, 2], [[1, 2], [3]]) SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- A tail that is not all NUL must still not match inside a container.
SELECT arrayExists(x -> x = ['V0abc'], [[toFixedString('V0', 3)]]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT arrayExists(x -> x = ['V0abc'], [[toFixedString('V0', 3)]]) SETTINGS optimize_rewrite_array_exists_to_has = 1;

-- Which spelling the plan ends up with. Match `function_name:` rather than the bare name:
-- the PROJECTION COLUMNS header echoes the original query text, so a bare `%arrayExists%`
-- matches even when the rewrite did happen.

-- Declined for a divergent pair: arrayExists stays, no has appears.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = 'V0\0', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = 'V0\0', v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = tuple('V0\0'), v) FROM t_tuple SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = tuple('V0\0'), v) FROM t_tuple SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';

-- Still rewritten where the two spellings provably agree.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = 'V0', v) FROM t_str SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = 'V0', v) FROM t_str SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = toFixedString('V0', 3), v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = toFixedString('V0', 3), v) FROM t_fs SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = [toFixedString('V0', 3)], v) FROM t_nested_arr SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = [toFixedString('V0', 3)], v) FROM t_nested_arr SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = map('k', toFixedString('V0', 3)), v) FROM t_nested_map SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = map('k', toFixedString('V0', 3)), v) FROM t_nested_map SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';

-- Declined for a mismatched pair reached through Array, Map, or a mix of containers.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = ['V0\0'], v) FROM t_nested_arr SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = ['V0\0'], v) FROM t_nested_arr SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = map('k', 'V0\0'), v) FROM t_nested_map SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = map('k', 'V0\0'), v) FROM t_nested_map SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = map('k\0', 'V'), [map(toFixedString('k', 2), 'V')]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = map('k\0', 'V'), [map(toFixedString('k', 2), 'V')]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = tuple(['V0\0']), [tuple([toFixedString('V0', 3)])]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = tuple(['V0\0']), [tuple([toFixedString('V0', 3)])]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';

-- Over a constant array too: a same-type tuple keeps the rewrite, a cross-width pair declines.
-- (A numeric constant array goes on to become `in` via the has -> in rewrite.)
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = tuple('V0'), [tuple('V0')]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = tuple('V0'), [tuple('V0')]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: has%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = 1, [1, 2]) SETTINGS optimize_rewrite_array_exists_to_has = 1) WHERE explain ILIKE '%function_name: arrayExists%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = 1, [1, 2]) SETTINGS optimize_rewrite_array_exists_to_has = 1, optimize_rewrite_has_to_in = 1) WHERE explain ILIKE '%function_name: in%';

-- Direct membership calls keep their current semantics: this change is only about the rewrite.
SELECT has(v, 'V0\0'), indexOf(v, 'V0\0'), countEqual(v, 'V0\0') FROM t_fs;

DROP TABLE t_fs;
DROP TABLE t_str;
DROP TABLE t_lc;
DROP TABLE t_null;
DROP TABLE t_tuple;
DROP TABLE t_tuple2;
DROP TABLE t_nested_arr;
DROP TABLE t_nested_map;
