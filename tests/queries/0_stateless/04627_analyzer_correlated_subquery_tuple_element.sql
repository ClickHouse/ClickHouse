-- Correlated subquery referencing an outer column through a subcolumn-producing function
-- (tupleElement / variantElement / Map arrayElement) used to fail with
-- NOT_FOUND_COLUMN_IN_BLOCK, because FunctionToSubcolumnsPass rewrote the outer column to a
-- subcolumn after the correlated-columns metadata had already been fixed to the whole column.
-- See https://github.com/ClickHouse/ClickHouse/issues/111729

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
-- The bug only manifests when FunctionToSubcolumnsPass runs, so pin it on: the CI runner otherwise
-- randomizes it and a run with it off would pass even on a reverted fix.
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_tuple (p Tuple(a Int32, b String)) ENGINE = Memory;
INSERT INTO t_tuple VALUES ((1, 'x')), ((2, 'y'));

-- Positional access on the outer column: o.p.1 (the reported case).
SELECT count() FROM t_tuple AS o WHERE EXISTS (SELECT 1 FROM t_tuple AS i WHERE i.p.a = o.p.1);
-- Positional via the tupleElement function.
SELECT count() FROM t_tuple AS o WHERE EXISTS (SELECT 1 FROM t_tuple AS i WHERE i.p.a = tupleElement(o.p, 1));
-- Named access via the tupleElement function (also went through the function path).
SELECT count() FROM t_tuple AS o WHERE EXISTS (SELECT 1 FROM t_tuple AS i WHERE i.p.a = tupleElement(o.p, 'a'));
-- Named identifier access: worked before, must keep working.
SELECT count() FROM t_tuple AS o WHERE EXISTS (SELECT 1 FROM t_tuple AS i WHERE i.p.a = o.p.a);
-- Positional on the inner side only: worked before, must keep working.
SELECT count() FROM t_tuple AS o WHERE EXISTS (SELECT 1 FROM t_tuple AS i WHERE i.p.1 = o.p.a);
-- Scalar correlated subquery variant.
SELECT count() FROM t_tuple AS o WHERE 1 = (SELECT count() FROM t_tuple AS i WHERE i.p.1 = o.p.1);

-- The same outer table's other, non-correlated column must still be optimized to a subcolumn:
-- tupleElement(o.q, 1) in the outer WHERE is rewritten to o.q.a while o.p.1 inside EXISTS is left
-- alone. Result must match the reference (the fix must not over-suppress).
DROP TABLE IF EXISTS t_two_tuples;
CREATE TABLE t_two_tuples (p Tuple(a Int32, b String), q Tuple(a Int32, b String)) ENGINE = Memory;
INSERT INTO t_two_tuples VALUES ((1, 'x'), (5, 'y')), ((2, 'z'), (9, 'w'));
SELECT count() FROM t_two_tuples AS o WHERE tupleElement(o.q, 1) = 5 AND EXISTS (SELECT 1 FROM t_two_tuples AS i WHERE i.p.a = o.p.1);

-- Prove the scope selectivity, not just the result: only the correlated column is skipped, every
-- other subcolumn rewrite (including columns of the SAME name inside the correlated subquery) still
-- fires. Here tupleElement(i.q, 1) is an inner-local (non-correlated) use and MUST be rewritten to
-- the subcolumn q.a (source of the inner table), while the correlated tupleElement(o.p, 1) is kept
-- as a function. A guard that suppressed every rewrite inside a correlated scope would fail the
-- first assertion; a guard that failed to protect the correlated column would fail the second.
SELECT
    countIf(explain ILIKE '%column_name: q.a%') > 0 AS inner_local_optimized,
    countIf(explain ILIKE '%function_name: tupleElement%') > 0 AS correlated_kept_as_function
FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM t_two_tuples AS o WHERE EXISTS (SELECT 1 FROM t_two_tuples AS i WHERE tupleElement(i.q, 1) = 5 AND i.p.a = o.p.1)
);

-- End-to-end coverage for a correlated subquery whose body is a UNION: each arm is its own
-- QueryNode, so both arms independently reference the outer o.p.1 and must keep it as a function.
SELECT count() FROM t_tuple AS o WHERE EXISTS (
    SELECT 1 FROM t_tuple AS i WHERE i.p.a = o.p.1
    UNION ALL
    SELECT 1 FROM t_tuple AS i WHERE i.p.a = tupleElement(o.p, 1)
);
-- Scope selectivity across both UNION arms: the inner-local tupleElement(i.q, 1) is rewritten to
-- q.a in each arm (2) while the correlated tupleElement(o.p, 1) is kept as a function in each arm (2).
SELECT
    countIf(explain ILIKE '%column_name: q.a%') AS inner_local_optimized_both_arms,
    countIf(explain ILIKE '%function_name: tupleElement%') AS correlated_kept_as_function
FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM t_two_tuples AS o WHERE EXISTS (
        SELECT 1 FROM t_two_tuples AS i WHERE tupleElement(i.q, 1) = 5 AND i.p.a = o.p.1
        UNION ALL
        SELECT 1 FROM t_two_tuples AS i WHERE tupleElement(i.q, 1) = 9 AND i.p.a = tupleElement(o.p, 1)
    )
);

-- Variant subcolumn via variantElement on the outer column.
SET allow_experimental_variant_type = 1;
DROP TABLE IF EXISTS t_variant;
CREATE TABLE t_variant (v Variant(Int32, String), k Int32) ENGINE = Memory;
INSERT INTO t_variant VALUES (7, 7), (3, 8);
SELECT count() FROM t_variant AS o WHERE EXISTS (SELECT 1 FROM t_variant AS i WHERE i.k = variantElement(o.v, 'Int32'));
-- Scope selectivity for Variant: the inner-local variantElement(i.v, 'Int32') IS rewritten to the
-- v.Int32 subcolumn while the correlated variantElement(o.v, 'Int32') is kept as a function.
SELECT
    countIf(explain ILIKE '%column_name: v.Int32%') AS inner_local_rewritten,
    countIf(explain ILIKE '%function_name: variantElement%') AS correlated_kept_as_function
FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM t_variant AS o WHERE EXISTS (SELECT 1 FROM t_variant AS i WHERE variantElement(i.v, 'Int32') = i.k AND i.k = variantElement(o.v, 'Int32'))
);

-- Map key access (arrayElement) on the outer column.
DROP TABLE IF EXISTS t_map;
CREATE TABLE t_map (m Map(String, Int32), k Int32) ENGINE = Memory;
INSERT INTO t_map VALUES ({'x': 9}, 9), ({'x': 4}, 8);
SELECT count() FROM t_map AS o WHERE EXISTS (SELECT 1 FROM t_map AS i WHERE i.k = o.m['x']);
-- Scope selectivity for Map: the inner-local i.m['x'] IS rewritten to the m.key_x subcolumn while
-- the correlated o.m['x'] is kept as arrayElement.
SELECT
    countIf(explain ILIKE '%m.key_x%') AS inner_local_rewritten,
    countIf(explain ILIKE '%function_name: arrayElement%') AS correlated_kept_as_function
FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM t_map AS o WHERE EXISTS (SELECT 1 FROM t_map AS i WHERE i.m['x'] = i.k AND i.k = o.m['x'])
);

DROP TABLE t_tuple;
DROP TABLE t_two_tuples;
DROP TABLE t_variant;
DROP TABLE t_map;
