-- `optimize_redundant_comparisons` gates comparison-chain pruning, boundary folding and range
-- strengthening, all introduced together with the setting. Detecting that two `equals` on the same
-- expression carry different values, and folding the AND to `false`, is older: it ran unconditionally
-- before the setting existed. Leaving that fold behind the setting made `compatibility` below the
-- release that added it, and an explicit `optimize_redundant_comparisons = 0`, execute the full plan
-- of a query whose filter is always false.
-- Every query pins `optimize_and_compare_chain` (the test runner randomizes it, and it derives
-- transitive conjuncts that change node counts) and `enable_analyzer = 1` (the pass is analyzer-only).

-- 1) The fold happens with pruning disabled. Counting `equals` nodes rather than matching a constant's
--    rendered value: 'Low' and 'Medium' also appear in the fixture's own arguments.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT s FROM values('s String', ('Low'), ('Medium')) WHERE (s = 'Low') AND (s = 'Medium') SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: equals,%';
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT s FROM values('s String', ('Low'), ('Medium')) WHERE (s = 'Low') AND (s = 'Medium') SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: equals,%';
-- A run of identical `equals` before the conflicting one: the conflict is found against one stored
-- representative, whose slot must survive the growth of the filter list.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT s FROM values('s String', ('Low'), ('Medium')) WHERE (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Low') AND (s = 'Medium') SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: equals,%';

-- 2) The same fold through the reported carrier: `compatibility` below the release that added the
--    setting resolves it to `false`.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT s FROM values('s String', ('Low'), ('Medium')) WHERE (s = 'Low') AND (s = 'Medium') SETTINGS enable_analyzer = 1, compatibility = '25.12', optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: equals,%';

-- 3) The reported shape: a LEFT JOIN onto an aggregated subquery, filtered by an always-false pair of
--    `equals` on the same expression. The fold must keep the aggregation from running at all, not
--    merely return no rows -- `throwIf` in the subquery's own WHERE cannot be pruned away, so the
--    query raises FUNCTION_THROW_IF_VALUE_IS_NON_ZERO if the right side is read.
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
CREATE TABLE t_left (k String, sev String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_right (k String, sev String, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_left SELECT toString(number), 'Low' FROM numbers(200);
INSERT INTO t_right SELECT toString(number % 100), 'Low', number FROM numbers(500);
SELECT count() FROM (SELECT m.k FROM t_left AS m LEFT JOIN (SELECT k, argMax(sev, v) AS sev FROM t_right WHERE throwIf(v >= 0, 'right side executed') GROUP BY k) AS p ON m.k = p.k WHERE (ifNull(p.sev, m.sev) = 'Low') AND (ifNull(p.sev, m.sev) = 'Medium') SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0);
SELECT count() FROM (SELECT m.k FROM t_left AS m LEFT JOIN (SELECT k, argMax(sev, v) AS sev FROM t_right WHERE throwIf(v >= 0, 'right side executed') GROUP BY k) AS p ON m.k = p.k WHERE (ifNull(p.sev, m.sev) = 'Low') AND (ifNull(p.sev, m.sev) = 'Medium') SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0);
DROP TABLE t_left;
DROP TABLE t_right;

-- 4) Two `equals` conflict only when their values differ in the column's type, never merely because a
--    second `equals` arrives: `1` and `1.0` are the same Float64, so the row survives.
SELECT groupArray(x) FROM (SELECT x FROM values('x Float64', (1.0), (2.0)) WHERE (x = 1) AND (x = 1.0) ORDER BY x SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0);
SELECT groupArray(x) FROM (SELECT x FROM values('x Float64', (1.0), (2.0)) WHERE (x = 1) AND (x = 1.0) ORDER BY x SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0);

-- 5) A conflict does not collapse an AND that also holds a comparison whose constant cannot be
--    converted: executing it raises TYPE_MISMATCH, and dropping it would make the error depend on the
--    setting. The convertible counterpart below shows the conflict is otherwise found on both settings.
SELECT count() FROM values('i Int32', (1)) WHERE (i = 1) AND (i = 2) AND (i > 'str') SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0; -- { serverError TYPE_MISMATCH }
SELECT count() FROM values('i Int32', (1)) WHERE (i = 1) AND (i = 2) AND (i > 'str') SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0; -- { serverError TYPE_MISMATCH }
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT i FROM values('i Int32', (1)) WHERE (i = 1) AND (i = 2) AND (i > 0) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: equals,%';
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT i FROM values('i Int32', (1)) WHERE (i = 1) AND (i = 2) AND (i > 0) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: equals,%';

-- 6) Only the equals/equals conflict is ungated. A contradiction between two ranges is part of what
--    the setting introduced and stays gated: one `less` survives with pruning off, none with it on.
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT a FROM values('a Int32', (3)) WHERE (a < 1) AND (a > 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: less,%';
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT a FROM values('a Int32', (3)) WHERE (a < 1) AND (a > 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: less,%';

-- 7) `optimize_and_compare_chain` appends a contradiction it derives (`x = 3 AND x = y AND y = 5`
--    yields `x = 5`) as a plain conjunct, for this pass to fold. With pruning off that conjunct is now
--    folded too, so the derived contradiction collapses the AND; with the chain pass off nothing is
--    derived and nothing collapses. The result is empty in every combination either way.
SELECT count() FROM values('x Int32, y Int32', (3, 5), (3, 3)) WHERE (x = 3) AND (x = y) AND (y = 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;
SELECT count() FROM values('x Int32, y Int32', (3, 5), (3, 3)) WHERE (x = 3) AND (x = y) AND (y = 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM values('x Int32, y Int32', (3, 5), (3, 3)) WHERE (x = 3) AND (x = y) AND (y = 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0;
SELECT count() FROM values('x Int32, y Int32', (3, 5), (3, 3)) WHERE (x = 3) AND (x = y) AND (y = 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT x FROM values('x Int32, y Int32', (3, 5), (3, 3)) WHERE (x = 3) AND (x = y) AND (y = 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: and,%';
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT x FROM values('x Int32, y Int32', (3, 5), (3, 3)) WHERE (x = 3) AND (x = y) AND (y = 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1) WHERE explain ILIKE '%function_name: and,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT x FROM values('x Int32, y Int32', (3, 5), (3, 3)) WHERE (x = 3) AND (x = y) AND (y = 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: and,%';
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT x FROM values('x Int32, y Int32', (3, 5), (3, 3)) WHERE (x = 3) AND (x = y) AND (y = 5) SETTINGS enable_analyzer = 1, optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1) WHERE explain ILIKE '%function_name: and,%';
