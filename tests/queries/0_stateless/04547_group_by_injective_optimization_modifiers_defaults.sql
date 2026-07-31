-- Regression for #110715 / follow-up to #110721: keep the injective-function GROUP BY optimization
-- (optimize_injective_functions_in_group_by) enabled under GROUP BY modifiers while emitting the correct
-- output default for absent-key rows, instead of recomputing f(column-default). Every result below must be
-- identical with the optimization ON and OFF; the EXPLAIN assertion verifies the optimization still fires.
-- Every setting that affects these results is pinned per query, so the test is parallel-safe and does not
-- rely on any default randomization.

SET enable_analyzer = 1;
SET optimize_injective_functions_in_group_by = 1;
SET group_by_use_nulls = 0;
SET force_grouping_standard_compatibility = 1;

SELECT '-- plain WITH TOTALS: totals row is the String default, not toString(0)';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS ORDER BY s;

SELECT '-- GROUPING SETS with a non-member () set: the () row is the String default';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), ())
ORDER BY grouping(toString(number)) DESC, s;

SELECT '-- GROUPING SETS ((k),(k,k)) where the injective key repeats: grouping() must reference the';
SELECT '-- unwrapped key, so the rewritten tree stays valid (it is re-analyzed on distributed shards)';
SELECT count() AS c, toString(number) AS k FROM numbers(5)
GROUP BY GROUPING SETS ((k), (k, k))
ORDER BY k, c;

SELECT '-- ROLLUP: the rolled-up () level is the String default';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number))
ORDER BY grouping(toString(number)) DESC, s;

SELECT '-- CUBE: the () level is the String default';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY CUBE(toString(number))
ORDER BY grouping(toString(number)) DESC, s;

SELECT '-- ROLLUP ... WITH TOTALS: subtotal rows via grouping conditional, grand total via totals port';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number)) WITH TOTALS
ORDER BY grouping(toString(number)) DESC, s;

SELECT '-- GROUPING SETS with force_grouping_standard_compatibility = 0 (the other grouping() branch)';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), ())
ORDER BY grouping(toString(number)) DESC, s
SETTINGS force_grouping_standard_compatibility = 0;

SELECT '-- type matrix under plain WITH TOTALS where defaultOf(resultType) != f(default)';
SELECT reinterpretAsString(number) AS s, count() AS c FROM numbers(3)
GROUP BY reinterpretAsString(number) WITH TOTALS ORDER BY s;
SELECT toFixedString(toString(number), 4) AS s, count() AS c FROM numbers(3)
GROUP BY toFixedString(toString(number), 4) WITH TOTALS ORDER BY s;
SELECT toLowCardinality(toString(number)) AS s, count() AS c FROM numbers(3)
GROUP BY toLowCardinality(toString(number)) WITH TOTALS ORDER BY s;

SELECT '-- negative guard (CASE B): user groups by the argument and projects f(argument);';
SELECT '-- here the correct totals value IS toString(0) = 0 and must be unchanged';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY number WITH TOTALS ORDER BY s;

SELECT '-- group_by_use_nulls = 1: optimization is not applied, result stays correct';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS group_by_use_nulls = 1;

SELECT '-- the optimization still fires: the aggregation key is the unwrapped column, not the function';
SELECT trimLeft(explain)
FROM (EXPLAIN PLAN
      SELECT toString(number) AS s, count() FROM numbers(3)
      GROUP BY toString(number) WITH TOTALS)
WHERE explain ILIKE '%Keys:%';

SELECT '-- the optimization still fires when the key is also referenced in ORDER BY (totals row is not sorted)';
SELECT trimLeft(explain)
FROM (EXPLAIN PLAN
      SELECT toString(number) AS s, count() FROM numbers(3)
      GROUP BY toString(number) WITH TOTALS ORDER BY s)
WHERE explain ILIKE '%Keys:%';

SELECT '-- optimization on == off: same result with the optimization disabled';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- collision across eliminations: two injective keys unwrapping to the same leaf must keep the';
SELECT '-- full CUBE lattice (they must not be merged into one aggregation key)';
SELECT toString(number) AS a, negate(number) AS b, count() AS c FROM numbers(2)
GROUP BY CUBE(toString(number), negate(number))
ORDER BY grouping(toString(number)) DESC, grouping(negate(number)) DESC, a, b, c;

SELECT '-- same collision under ROLLUP';
SELECT toString(number) AS a, negate(number) AS b, count() AS c FROM numbers(2)
GROUP BY ROLLUP(toString(number), negate(number))
ORDER BY grouping(toString(number)) DESC, grouping(negate(number)) DESC, a, b, c;

SELECT '-- window PARTITION BY on the eliminated key: the special row must use the type default,';
SELECT '-- so the key is kept wrapped (optimization declined for that key), result stays correct';
SELECT toString(number) AS k, count() OVER (PARTITION BY toString(number)) AS w, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), ())
ORDER BY k, w, c;

SELECT '-- QUALIFY referencing the eliminated key under GROUPING SETS: kept wrapped, result correct';
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), ())
QUALIFY first_value(k) OVER (ORDER BY k) = k
ORDER BY k, c;

SELECT '-- QUALIFY referencing the eliminated key under plain WITH TOTALS: kept wrapped, result correct';
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS
QUALIFY first_value(k) OVER (ORDER BY k) = k
ORDER BY k, c;

SELECT '-- window/QUALIFY-bound cases on == off';
SELECT toString(number) AS k, count() OVER (PARTITION BY toString(number)) AS w, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), ())
ORDER BY k, w, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS
QUALIFY first_value(k) OVER (ORDER BY k) = k
ORDER BY k, c
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- two same-named keys from different sources (self-join): each correction must be attributed to';
SELECT '-- its own aggregation key, so every mixed subtotal keeps the value of the side it belongs to.';
SELECT '-- Oracle: the same query with the optimization disabled (printed right below each).';
SELECT toString(l.number) AS a, toString(r.number) AS b, count() AS c
FROM numbers(2) AS l JOIN numbers(2) AS r ON l.number = r.number
GROUP BY CUBE(toString(l.number), toString(r.number)) ORDER BY a, b, c;
SELECT toString(l.number) AS a, toString(r.number) AS b, count() AS c
FROM numbers(2) AS l JOIN numbers(2) AS r ON l.number = r.number
GROUP BY CUBE(toString(l.number), toString(r.number)) ORDER BY a, b, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT toString(l.number) AS a, toString(r.number) AS b, count() AS c
FROM numbers(2) AS l JOIN numbers(2) AS r ON l.number = r.number
GROUP BY GROUPING SETS ((toString(l.number)), (toString(r.number)), (toString(l.number), toString(r.number)))
ORDER BY a, b, c;
SELECT toString(l.number) AS a, toString(r.number) AS b, count() AS c
FROM numbers(2) AS l JOIN numbers(2) AS r ON l.number = r.number
GROUP BY GROUPING SETS ((toString(l.number)), (toString(r.number)), (toString(l.number), toString(r.number)))
ORDER BY a, b, c
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- a user grouping() call over the eliminated key: its argument must become the unwrapped key, so';
SELECT '-- a distributed shard re-analyzing the serialized query still accepts it. remote() with the';
SELECT '-- default prefer_localhost_replica takes that serialize/re-analyze path.';
SELECT toString(number) AS k, grouping(toString(number)) AS g, count() AS c
FROM remote('127.0.0.1', numbers(3))
GROUP BY CUBE(toString(number)) ORDER BY k, g, c;
SELECT toString(number) AS k, grouping(toString(number)) AS g, count() AS c
FROM remote('127.0.0.1', numbers(3))
GROUP BY CUBE(toString(number)) ORDER BY k, g, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT '-- and locally, where the grouping() value must be unchanged by the substitution';
SELECT toString(number) AS k, grouping(toString(number)) AS g, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number)) ORDER BY k, g, c;
SELECT toString(number) AS k, grouping(toString(number)) AS g, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number)) ORDER BY k, g, c
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- LIMIT BY over the eliminated key: the subtotal row must not merge with a real group. Oracle:';
SELECT '-- the same query WITHOUT LIMIT BY, which is a no-op over all-distinct keys (the';
SELECT '-- optimization-disabled arm is unavailable here, it fails with NOT_FOUND_COLUMN_IN_BLOCK both';
SELECT '-- before and after this change). Counts are distinct so ORDER BY is a total order.';
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY CUBE(toString(x)) ORDER BY k, c LIMIT 1 BY k;
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY CUBE(toString(x)) ORDER BY k, c;
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY ROLLUP(toString(x)) ORDER BY k, c LIMIT 1 BY k;
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY GROUPING SETS ((toString(x)), ()) ORDER BY k, c LIMIT 1 BY k;

SELECT '-- INTERPOLATE over the eliminated key: a filled row must carry the String default, not';
SELECT '-- toString(0). The interpolated column must not be in ORDER BY (that is rejected), so this';
SELECT '-- orders by the count, made distinct so the row order is deterministic.';
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY CUBE(toString(x))
ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k);
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY CUBE(toString(x))
ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k)
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY ROLLUP(toString(x))
ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k);
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY ROLLUP(toString(x))
ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k)
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- INTERPOLATE under plain WITH TOTALS: the totals-port overwrite is a whole-column projection';
SELECT '-- overwrite and cannot reach the INTERPOLATE clause, so the key stays wrapped there (correct but';
SELECT '-- unoptimized). Result must still match the optimization-disabled arm.';
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY toString(x) WITH TOTALS
ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k);
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY toString(x) WITH TOTALS
ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k)
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- per-modifier firing assertion: 1 means the unwrap actually happened (no injective function is';
SELECT '-- left in the GROUP BY section of the analyzed tree). Without this the assertions above would';
SELECT '-- also pass if the optimization silently declined for every modifier.';
SELECT 'CUBE', countIf(explain LIKE '%toString%' AND rn > gb) = 0 FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '%GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3) GROUP BY CUBE(toString(number))));
SELECT 'ROLLUP', countIf(explain LIKE '%toString%' AND rn > gb) = 0 FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '%GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3) GROUP BY ROLLUP(toString(number))));
SELECT 'GROUPING SETS', countIf(explain LIKE '%toString%' AND rn > gb) = 0 FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '%GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY GROUPING SETS ((toString(number)), ())));
SELECT 'ROLLUP WITH TOTALS', countIf(explain LIKE '%toString%' AND rn > gb) = 0 FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '%GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY ROLLUP(toString(number)) WITH TOTALS));
SELECT 'WITH TOTALS', countIf(explain LIKE '%toString%' AND rn > gb) = 0 FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '%GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY toString(number) WITH TOTALS));
SELECT '-- the same assertion reads 0 when the optimization is off, so it is not vacuous';
SELECT 'CUBE off', countIf(explain LIKE '%toString%' AND rn > gb) = 0 FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '%GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3) GROUP BY CUBE(toString(number))
          SETTINGS optimize_injective_functions_in_group_by = 0));
