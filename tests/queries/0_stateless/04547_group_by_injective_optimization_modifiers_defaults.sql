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

SELECT '-- ROLLUP ... WITH TOTALS: every row here comes from the grouping conditional, including the';
SELECT '-- grand total (plain WITH TOTALS is declined outright, so the deleted mechanism never ran)';
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

SELECT '-- the same FixedString wrapper under CUBE, where the correction does run: the () level must be';
SELECT '-- four NUL bytes (00000000 in hex), not toFixedString(''0'', 4) = 30000000. hex() makes the';
SELECT '-- difference visible, which a raw FixedString column would not.';
SELECT hex(toFixedString(toString(number), 4)) AS h, count() AS c FROM numbers(3)
GROUP BY CUBE(toFixedString(toString(number), 4))
ORDER BY grouping(toFixedString(toString(number), 4)) DESC, h;
SELECT hex(toFixedString(toString(number), 4)) AS h, count() AS c FROM numbers(3)
GROUP BY CUBE(toFixedString(toString(number), 4))
ORDER BY grouping(toFixedString(toString(number), 4)) DESC, h
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- a LowCardinality-typed key under CUBE: `if` can never return LowCardinality, so the';
SELECT '-- correcting conditional would narrow the declared type. Such a key is declined (the firing';
SELECT '-- assertion below reads 0) and the result stays unoptimized but correct. Without the decline';
SELECT '-- the enclosing length()/hex() is handed the narrowed type and the query fails to analyze.';
DROP TABLE IF EXISTS t_04547_lc;
CREATE TABLE t_04547_lc (lc LowCardinality(String)) ENGINE = Memory;
INSERT INTO t_04547_lc VALUES ('ab'), ('cd');
SELECT 'lc cube on', reverse(lc) AS k, length(k) AS o, count() AS c FROM t_04547_lc
GROUP BY CUBE(reverse(lc)) ORDER BY k, c;
SELECT 'lc cube off', reverse(lc) AS k, length(k) AS o, count() AS c FROM t_04547_lc
GROUP BY CUBE(reverse(lc)) ORDER BY k, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT '-- and the declared type must not depend on the setting (both arms report LowCardinality(String))';
DESC (SELECT reverse(lc) AS k FROM t_04547_lc GROUP BY CUBE(reverse(lc)))
SETTINGS optimize_injective_functions_in_group_by = 1;
DESC (SELECT reverse(lc) AS k FROM t_04547_lc GROUP BY CUBE(reverse(lc)))
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- negative guard (CASE B): user groups by the argument and projects f(argument);';
SELECT '-- here the correct totals value IS toString(0) = 0 and must be unchanged';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY number WITH TOTALS ORDER BY s;

SELECT '-- group_by_use_nulls = 1: optimization is not applied, result stays correct';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS group_by_use_nulls = 1;

SELECT '-- under plain WITH TOTALS the key stays wrapped: the grand-total row has no __grouping_set column,';
SELECT '-- so the only possible correction lives outside the query tree and is lost when the tree is';
SELECT '-- converted to an AST (a shard receives text; the planner subquery cache keys on that AST).';
SELECT trimLeft(explain)
FROM (EXPLAIN PLAN
      SELECT toString(number) AS s, count() FROM numbers(3)
      GROUP BY toString(number) WITH TOTALS)
WHERE explain ILIKE '%Keys:%';

SELECT '-- same with the key also referenced in ORDER BY';
SELECT trimLeft(explain)
FROM (EXPLAIN PLAN
      SELECT toString(number) AS s, count() FROM numbers(3)
      GROUP BY toString(number) WITH TOTALS ORDER BY s)
WHERE explain ILIKE '%Keys:%';

SELECT '-- optimization on == off: same result with the optimization disabled';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- DISTRIBUTED plain WITH TOTALS. A shard receives the query as TEXT, so any correction recorded';
SELECT '-- beside the query tree cannot reach it; the shard would re-analyze GROUP BY <bare column> and';
SELECT '-- project f(default). Pinned on both prefer_localhost_replica values (the runner randomizes it)';
SELECT '-- and with serialize_query_plan = 0, so the distributed plan job (which sets it suite-wide) takes';
SELECT '-- the same serialize-as-text path these assertions describe.';
SELECT toString(number) AS s, count() AS c FROM remote('127.0.0.1', numbers(3))
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
SELECT toString(number) AS s, count() AS c FROM remote('127.0.0.1', numbers(3))
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0, optimize_injective_functions_in_group_by = 0;
SELECT toString(number) AS s, count() AS c FROM remote('127.0.0.1', numbers(3))
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS prefer_localhost_replica = 1, serialize_query_plan = 0;
SELECT toString(number) AS s, count() AS c FROM remote('127.0.0.1', numbers(3))
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS prefer_localhost_replica = 1, serialize_query_plan = 0, optimize_injective_functions_in_group_by = 0;

SELECT '-- the AST that a shard would receive must carry the aggregation key WRAPPED. This is the';
SELECT '-- structural form of the assertion above: it reads the converted query, not the result, so it';
SELECT '-- cannot be satisfied by a correction that only exists in the query tree.';
SELECT countIf(explain LIKE '%GROUP BY%toString%') = 1
FROM (EXPLAIN QUERY TREE run_passes = 1, dump_ast = 1
      SELECT toString(number) AS s, count() AS c FROM numbers(3)
      GROUP BY toString(number) WITH TOTALS);

SELECT '-- planner subquery result cache. These two queries need DIFFERENT totals values ('''' vs ''0''),';
SELECT '-- so if the key were unwrapped they would converge on one byte-identical AST and share a cache';
SELECT '-- entry, and whichever ran first would poison the other. Run as separate statements so the';
SELECT '-- second one probes an already-warm cache.';
SELECT * FROM (SELECT toString(number) AS s, count() AS c FROM numbers(3) GROUP BY toString(number) WITH TOTALS)
ORDER BY s SETTINGS query_cache_for_subqueries = 1, use_query_cache = 1;
SELECT * FROM (SELECT toString(number) AS s, count() AS c FROM numbers(3) GROUP BY number WITH TOTALS)
ORDER BY s SETTINGS query_cache_for_subqueries = 1, use_query_cache = 1;

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

SELECT '-- INTERPOLATE under plain WITH TOTALS: the key stays wrapped because plain WITH TOTALS is';
SELECT '-- declined outright (correct but unoptimized). Result must still match the disabled arm.';
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY toString(x) WITH TOTALS
ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k);
SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
GROUP BY toString(x) WITH TOTALS
ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k)
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- HAVING over the eliminated key. The predicate is evaluated after aggregation, so it must see';
SELECT '-- the corrected value: HAVING k = '''' selects the subtotal row, and HAVING k = ''0'' must not.';
SELECT '-- Each is paired with its optimization-disabled oracle. Every row carries its own label: the';
SELECT '-- two CUBE cells are complementary, so an unlabelled pair would let a row lost by one and a row';
SELECT '-- gained by the other cancel out in the line diff.';
SELECT 'cube-empty', toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY CUBE(toString(number)) HAVING k = '' ORDER BY k, c;
SELECT 'cube-empty off', toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY CUBE(toString(number)) HAVING k = '' ORDER BY k, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT 'cube-zero', toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY CUBE(toString(number)) HAVING k = '0' ORDER BY k, c;
SELECT 'cube-zero off', toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY CUBE(toString(number)) HAVING k = '0' ORDER BY k, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT '-- same through the expression rather than the alias, under ROLLUP';
SELECT 'rollup-expr', toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number)) HAVING toString(number) = '' ORDER BY k, c;
SELECT 'rollup-expr off', toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number)) HAVING toString(number) = '' ORDER BY k, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT '-- and under GROUPING SETS';
SELECT 'grouping-sets', toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), ()) HAVING k = '' ORDER BY k, c;
SELECT 'grouping-sets off', toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), ()) HAVING k = '' ORDER BY k, c
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT '-- a CONSTANT GROUP BY key makes the whole query decline. The planner drops such a key when the';
SELECT '-- query has aggregates, and whether it does so also depends on initiator-vs-shard flags this';
SELECT '-- pass cannot read, so the key count the grouping conditional is built from would be too large';
SELECT '-- and the conditional would mis-decide present-vs-absent. Declining matches master, which skips';
SELECT '-- the whole modifier family. Each shape is paired with its optimization-disabled oracle.';
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY CUBE(toString(number), toUInt8(7)) ORDER BY k, c;
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY CUBE(toString(number), toUInt8(7)) ORDER BY k, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number), toUInt8(7)) ORDER BY k, c;
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number), toUInt8(7)) ORDER BY k, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT '-- GROUPING SETS takes a different pruning branch in the planner, so cover it too: once with the';
SELECT '-- constant beside the injective key, and once as the only key of its own set.';
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number), toUInt8(7)), ()) ORDER BY k, c;
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number), toUInt8(7)), ()) ORDER BY k, c
SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), (toUInt8(7))) ORDER BY k, c;
SELECT toString(number) AS k, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), (toUInt8(7))) ORDER BY k, c
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
SELECT '-- plain WITH TOTALS reads 0: it is DECLINED on purpose (its correction cannot survive the';
SELECT '-- conversion of the query tree to an AST). The grouping-set modifiers above still read 1.';
SELECT 'WITH TOTALS declined', countIf(explain LIKE '%toString%' AND rn > gb) = 0 FROM (
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

SELECT '-- firing assertions for the four special-path shapes above, whose other assertions compare';
SELECT '-- results against the optimization-disabled arm and would therefore also pass if the pass had';
SELECT '-- silently declined for that shape. The window is the GROUP BY SECTION only (up to the next';
SELECT '-- section header), because the correction deliberately puts toString back into ORDER BY /';
SELECT '-- INTERPOLATE / the projection. Each is paired with its own off arm, which must read 0.';
SELECT 'self-join', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0
       AND countIf(explain LIKE '%COLUMN%source_id%' AND rn > gb AND rn < nxt) = 2
       AND uniqExact(if(explain LIKE '%COLUMN%source_id%' AND rn > gb AND rn < nxt,
                        extract(explain, 'source_id: (\\d+)'), NULL)) = 2 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(l.number) AS a, toString(r.number) AS b, count() AS c
          FROM numbers(2) AS l JOIN numbers(2) AS r ON l.number = r.number
          GROUP BY CUBE(toString(l.number), toString(r.number)))));
SELECT 'self-join off', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(l.number) AS a, toString(r.number) AS b, count() AS c
          FROM numbers(2) AS l JOIN numbers(2) AS r ON l.number = r.number
          GROUP BY CUBE(toString(l.number), toString(r.number))
          SETTINGS optimize_injective_functions_in_group_by = 0)));
SELECT 'user grouping()', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, grouping(toString(number)) AS g, count() AS c FROM numbers(3)
          GROUP BY ROLLUP(toString(number)))));
SELECT 'user grouping() off', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, grouping(toString(number)) AS g, count() AS c FROM numbers(3)
          GROUP BY ROLLUP(toString(number))
          SETTINGS optimize_injective_functions_in_group_by = 0)));
SELECT 'LIMIT BY', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
          GROUP BY CUBE(toString(x)) ORDER BY k, c LIMIT 1 BY k)));
SELECT 'LIMIT BY off', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
          GROUP BY CUBE(toString(x)) ORDER BY k, c LIMIT 1 BY k
          SETTINGS optimize_injective_functions_in_group_by = 0)));
SELECT 'INTERPOLATE', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
          GROUP BY CUBE(toString(x))
          ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k))));
SELECT 'INTERPOLATE off', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(x) AS k, count() AS c FROM (SELECT arrayJoin([0, 1, 1, 2, 2, 2]) AS x)
          GROUP BY CUBE(toString(x))
          ORDER BY c WITH FILL FROM 1 TO 8 INTERPOLATE (k AS k)
          SETTINGS optimize_injective_functions_in_group_by = 0)));
SELECT 'HAVING', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY CUBE(toString(number)) HAVING k = '')));
SELECT 'HAVING off', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY CUBE(toString(number)) HAVING k = ''
          SETTINGS optimize_injective_functions_in_group_by = 0)));

SELECT '-- the constant-key decline is targeted: the shape with a constant key reads 0 (declined) while a';
SELECT '-- sibling with the SAME predicate whose second key is a non-constant function still reads 1, so';
SELECT '-- the decline has not silently disabled the whole modifier family.';
SELECT 'constant key declined', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY CUBE(toString(number), toUInt8(7)))));
SELECT 'non-constant sibling fires', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY CUBE(toString(number), toUInt8(number)))));
SELECT 'constant key declined, GROUPING SETS', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY GROUPING SETS ((toString(number), toUInt8(7)), ()))));

SELECT '-- the LowCardinality key reads 0 (declined by the type-preservation gate) while the same shape';
SELECT '-- over a plain String column reads 1, so the gate is targeted at the type that `if` narrows.';
SELECT 'lc key declined', countIf(explain LIKE '%reverse%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT reverse(lc) AS k, count() AS c FROM t_04547_lc GROUP BY CUBE(reverse(lc)))));
SELECT 'non-lc sibling fires', countIf(explain LIKE '%reverse%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT reverse(s) AS k, count() AS c FROM (SELECT toString(number) AS s FROM numbers(3))
          GROUP BY CUBE(reverse(s)))));
SELECT '-- and the FixedString wrapper under CUBE fires, so its subtotal really comes from the correction';
SELECT 'fixed string cube fires', countIf(explain LIKE '%toFixedString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT hex(toFixedString(toString(number), 4)) AS h, count() AS c FROM numbers(3)
          GROUP BY CUBE(toFixedString(toString(number), 4)))));

SELECT '-- every firing assertion above inherits force_grouping_standard_compatibility = 1 from the';
SELECT '-- file-level SET, but the grouping conditional compares against a different constant on the other';
SELECT '-- branch. This pair covers it: a silent decline there would still produce correct results, so the';
SELECT '-- reference diff alone cannot catch it.';
SELECT 'compatibility 0 fires', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY GROUPING SETS ((toString(number)), ())
          SETTINGS force_grouping_standard_compatibility = 0)));
SELECT 'compatibility 0 off', countIf(explain LIKE '%toString%' AND rn > gb AND rn < nxt) = 0 FROM (
    SELECT explain, rn, gb, min(if(rn > gb AND match(explain, '^  [A-Z]'), rn, 999999)) OVER () AS nxt FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn,
           min(if(explain LIKE '  GROUP BY%', rowNumberInAllBlocks(), 999999)) OVER () AS gb
    FROM (EXPLAIN QUERY TREE run_passes = 1
          SELECT toString(number) AS k, count() AS c FROM numbers(3)
          GROUP BY GROUPING SETS ((toString(number)), ())
          SETTINGS force_grouping_standard_compatibility = 0,
                   optimize_injective_functions_in_group_by = 0)));

DROP TABLE t_04547_lc;
