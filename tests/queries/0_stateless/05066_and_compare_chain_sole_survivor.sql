-- The `AND` comparison chain optimization prunes always-true comparisons. When one operand is left
-- it replaces the whole `AND`, so it must still evaluate that operand as a boolean, and it must
-- leave a `GROUP BY` key alone when the key and its copies elsewhere in the query are typed
-- differently.
SET optimize_redundant_comparisons = 1;
SET enable_analyzer = 1;

-- A non-boolean sole survivor has to be evaluated as a boolean, not returned verbatim.
SELECT materialize(2::UInt8) AND (materialize(7::UInt8) < 300);
SELECT materialize(2::UInt8)::LowCardinality(UInt8) AND (materialize(7::UInt8) < 300) SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT sum(reinterpret(materialize(2::UInt8), 'Bool') AND (materialize(7::UInt8) < 300));
SELECT materialize(256::UInt32) AND (materialize(7::UInt8) < 300);
SELECT materialize(true) AND (materialize(7::UInt8) < 300) AS v, toTypeName(v);
-- The `AND` is kept and given its identity operand, so the survivor is booleanized without anything
-- being wrapped around it.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1
SELECT materialize(2::UInt8) AND (materialize(7::UInt8) < 300)) WHERE explain ILIKE '%function_name: and%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1
SELECT materialize(2::UInt8) AND (materialize(7::UInt8) < 300)) WHERE explain ILIKE '%function_name: notEquals%';

-- A survivor that is already a boolean function stands in for the `AND` on its own.
SELECT (materialize(7::UInt8) = 7) AND (materialize(7::UInt8) < 300);
SELECT (materialize(7::UInt8) IN (7)) AND (materialize(7::UInt8) < 300);
SELECT isNull(materialize(7::UInt8)) AND (materialize(7::UInt8) < 300);
SELECT not(materialize(true)) AND (materialize(7::UInt8) < 300) AS v, toTypeName(v);
SELECT count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0 GROUP BY (c0 = 1) = 1 WITH ROLLUP;
-- A query at a guarded level whose `or` and `equals` sites are untouched still returns correct
-- values. It holds no `AND`, so it has no sole survivor of its own.
SELECT count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0 GROUP BY (c0 = true OR u = 7) WITH ROLLUP SETTINGS group_by_use_nulls = 1;
-- Such a survivor keeps the plan it has today: no `AND` is left around it, and the same probe
-- returns one when the optimization does not run, so its absence is what the first row asserts.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1
SELECT (materialize(7::UInt8) = 7) AND (materialize(7::UInt8) < 300)) WHERE explain ILIKE '%function_name: and%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1
SELECT (materialize(7::UInt8) = 7) AND (materialize(7::UInt8) < 300)
SETTINGS optimize_redundant_comparisons = 0) WHERE explain ILIKE '%function_name: and%';

-- Keeping the `AND` is also what keeps a skip index reachable: the index builders resolve a key
-- column from an atom's own arguments, so an atom that ends up inside another function is dropped
-- and the index is not consulted at all.
DROP TABLE IF EXISTS t_05066;
CREATE TABLE t_05066 (msg String, severity UInt8,
    INDEX idx_msg msg TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_05066 SELECT if(number = 5, 'needle', 'hay'), 1 FROM numbers(32);
SELECT count() > 0 FROM (EXPLAIN indexes = 1
    SELECT count() FROM t_05066 WHERE hasToken(msg, 'needle') AND (severity < 300)
    SETTINGS use_skip_indexes = 1) WHERE explain ILIKE '%idx_msg%';
SELECT count() FROM t_05066 WHERE hasToken(msg, 'needle') AND (severity < 300);
DROP TABLE t_05066;

-- Reducing such a key to one of its operands makes it disagree with the copies the analyzer typed
-- against it, which under group_by_use_nulls are wrapped in `Nullable`.
SELECT c0 AND (u < 300) AS k, count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY c0 AND (u < 300) WITH ROLLUP ORDER BY k SETTINGS group_by_use_nulls = 1;

-- The same query with a comparison that is not always true, so nothing is pruned.
SELECT c0 AND (u < 5) AS k, count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY c0 AND (u < 5) WITH ROLLUP ORDER BY k SETTINGS group_by_use_nulls = 1;

SELECT c0 AND (u < 300) AS k, count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY c0 AND (u < 300) ORDER BY k SETTINGS group_by_use_nulls = 1;

-- A key whose surviving operand is a comparison is reduced as well.
SELECT (x = 3) AND (u < 300) AS k, count() FROM (SELECT 3::UInt8 AS x, 7::UInt8 AS u) t0
GROUP BY (x = 3) AND (u < 300) WITH ROLLUP ORDER BY k SETTINGS group_by_use_nulls = 1;

-- Copies of such a key also live outside the `GROUP BY` list: `grouping` keeps the key expression as
-- an argument, and an aggregate function over a key is replaced by a copy of the key.
SELECT grouping(c0 AND (u < 300)) AS g, count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY c0 AND (u < 300) WITH ROLLUP ORDER BY g SETTINGS group_by_use_nulls = 1;
SELECT grouping(c0 AND (u < 300)) AS g, count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY c0 AND (u < 300) WITH CUBE ORDER BY g SETTINGS group_by_use_nulls = 1;
SELECT grouping(c0 AND (u < 300)) AS g, count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY GROUPING SETS ((c0 AND (u < 300)), ()) ORDER BY g SETTINGS group_by_use_nulls = 1;
SELECT min(c0 AND (u < 300)) AS m, count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY GROUPING SETS ((c0 AND (u < 300)))
SETTINGS group_by_use_nulls = 1, optimize_aggregators_of_group_by_keys = 1;

-- Transitive inference is the other optimization that reduces such a key, and it is left out of it
-- as well. It wraps what it derives in `indexHint`, so the absence of that is what marks it as not
-- having run. The first two keys are the same chain with one of the two conditions for leaving the
-- key alone removed, which is what makes the third one's absence attributable to the key and not to
-- the chain being uninformative.
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10))
GROUP BY (x < y) AND (y < 3)
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 1) WHERE explain ILIKE '%indexHint%';
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10))
GROUP BY (x < y) AND (y < 3) WITH ROLLUP
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 0) WHERE explain ILIKE '%indexHint%';
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10))
GROUP BY (x < y) AND (y < 3) WITH ROLLUP
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 1) WHERE explain ILIKE '%indexHint%';

-- A correlated reference to an operand of the key is rejected, as it is when nothing is pruned.
SELECT (SELECT c0) FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY c0 AND (u < 300) WITH ROLLUP SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }
SELECT (SELECT c0) FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY c0 AND (u < 300) WITH CUBE SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }
SELECT (SELECT c0) FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY GROUPING SETS ((c0 AND (u < 300)), ()) SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }
-- A copy of the key inside a nested query is not one of the `Nullable` copies, and reaches the same
-- rejection.
SELECT (SELECT c0 AND (u < 300)) FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY c0 AND (u < 300) WITH ROLLUP SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }
-- The same rejection where nothing is pruned, which is what the claim above is measured against.
SELECT (SELECT c0) FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
GROUP BY c0 AND (u < 5) WITH ROLLUP SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }

-- What is left alone is a query level, so a chain at a nested level is still optimized while the
-- outer key is not: the flag is per level and is restored on the way back out. One `EXPLAIN` holds
-- both levels, so each probe names the `indexHint` its own level derives and neither level's hint
-- can stand in for the other's. The third row is the outer pattern with the key left unguarded,
-- which is what makes the second row's zero attributable to the guard and not to a pattern that
-- matches nothing.
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) t0
WHERE t0.x IN (SELECT count() FROM (SELECT number AS a, number * 2 AS b FROM numbers(10)) WHERE (a < b) AND (b < 3))
GROUP BY (t0.x < t0.y) AND (t0.y < 3) WITH ROLLUP
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 1) WHERE explain ILIKE '%indexHint(less(__table1.a, 3))%';
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) t0
WHERE t0.x IN (SELECT count() FROM (SELECT number AS a, number * 2 AS b FROM numbers(10)) WHERE (a < b) AND (b < 3))
GROUP BY (t0.x < t0.y) AND (t0.y < 3) WITH ROLLUP
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 1) WHERE explain ILIKE '%indexHint(less(__table1.x, 3))%';
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) t0
WHERE t0.x IN (SELECT count() FROM (SELECT number AS a, number * 2 AS b FROM numbers(10)) WHERE (a < b) AND (b < 3))
GROUP BY (t0.x < t0.y) AND (t0.y < 3) WITH ROLLUP
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 0) WHERE explain ILIKE '%indexHint(less(__table1.x, 3))%';

-- Other clauses of a query that has such a key return the same results.
SELECT count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0 WHERE c0 AND (u < 300)
GROUP BY u WITH ROLLUP SETTINGS group_by_use_nulls = 1;
SELECT count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
JOIN (SELECT 1::Bool AS d0) t1 ON (t0.c0 = t1.d0) AND (t0.u < 300)
GROUP BY t0.u WITH ROLLUP SETTINGS group_by_use_nulls = 1;
-- An alias makes one node serve two clauses, so the clause an `and` sits in does not decide whether
-- it may be moved: `WHERE k` and `GROUP BY k` are the same node, and moving it there moves the key.
SELECT c0 AND (u < 300) AS k, count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
WHERE k GROUP BY k WITH ROLLUP ORDER BY k SETTINGS group_by_use_nulls = 1;
WITH (c0 AND (u < 300)) AS e
SELECT e, count() FROM (SELECT 1::Bool AS c0, 7::UInt8 AS u) t0
WHERE e GROUP BY e WITH ROLLUP ORDER BY e SETTINGS group_by_use_nulls = 1;
-- Their own chains are not optimized either, because a query level is what is left alone. Each row
-- has a group_by_use_nulls = 0 sibling, so the absence is attributable to the setting and not to the
-- chain being uninformative.
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10))
WHERE (x < y) AND (y < 3) GROUP BY x WITH ROLLUP
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 1) WHERE explain ILIKE '%indexHint%';
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10))
WHERE (x < y) AND (y < 3) GROUP BY x WITH ROLLUP
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 0) WHERE explain ILIKE '%indexHint%';
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) t0
JOIN (SELECT number AS z FROM numbers(10)) t1 ON (t1.z = t0.x) AND (t0.x < t0.y) AND (t0.y < 3)
GROUP BY t0.x WITH ROLLUP
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 1) WHERE explain ILIKE '%indexHint%';
SELECT count() > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) t0
JOIN (SELECT number AS z FROM numbers(10)) t1 ON (t1.z = t0.x) AND (t0.x < t0.y) AND (t0.y < 3)
GROUP BY t0.x WITH ROLLUP
SETTINGS optimize_and_compare_chain = 1, group_by_use_nulls = 0) WHERE explain ILIKE '%indexHint%';
