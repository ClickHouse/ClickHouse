-- https://github.com/ClickHouse/ClickHouse/issues/91119
--
-- Regression coverage for the analyzer change in PR #104350. When walking
-- enclosing scopes to apply `group_by_use_nulls` Nullable wrapping, the
-- aggregate-function suppression must remain OR-accumulated across the chain
-- of LAMBDA scopes up to (and including) the enclosing `QUERY` scope. A
-- per-scope check breaks this for `* APPLY x -> agg(x, key)` queries: the
-- LAMBDA scope sees the aggregate on its stack but its own
-- `nullable_group_by_keys` is empty; the outer `QUERY` scope holds the keys
-- but its own stack does not see the aggregate (the aggregate lives inside
-- the lambda body), so a per-scope check would wrap the inner reference
-- as `Nullable` and the aggregate's per-group input column type would no
-- longer match the function's signature, causing an exception (or, for some
-- aggregates, a memory access fault inside `addBatchSinglePlace`).
--
-- The cases below all combine `group_by_use_nulls = 1` with `* APPLY <lambda>`
-- where the lambda body contains an aggregate that references a GROUP BY key,
-- across the four GROUP BY shapes (`ROLLUP`, `CUBE`, `GROUPING SETS`, plain).
-- Each must run to completion and produce the expected output.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT '-- GROUPING SETS, materialize key, argMax ---';
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY GROUPING SETS ((materialize(65537)), (*));

SELECT '-- ROLLUP, argMax ---';
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY number WITH ROLLUP;

SELECT '-- CUBE, argMax ---';
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY number WITH CUBE;

SELECT '-- ROLLUP, sum ---';
SELECT * APPLY x -> sum(x) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

SELECT '-- ROLLUP, function-form aggregate ---';
SELECT * APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

SELECT '-- ROLLUP, regex-filtered APPLY ---';
SELECT (SELECT * APPLY (x -> argMax(x, number), 'f_'))
FROM numbers(256) GROUP BY * WITH ROLLUP WITH TOTALS LIMIT -9223372036854775807, 225;

SELECT '-- nested aggregate inside a function ---';
SELECT * APPLY x -> toString(argMax(x, number))
FROM numbers(1) GROUP BY GROUPING SETS ((materialize(65537)), (*));

SELECT '-- deeply nested aggregate ---';
SELECT * APPLY x -> length(toString(sum(x)))
FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

-- A `grouping(...)` argument in an APPLY transformer must be suppressed the same
-- way an aggregate argument is: `grouping` only identifies a GROUP BY key and is
-- matched against the keys in their original (non-Nullable) form by
-- GroupingFunctionsResolvePass. Wrapping the matched key Nullable here would make
-- the rewritten `grouping(key)` stop matching and raise a spurious
-- "GROUPING function ... is not in GROUP BY keys" error. Covers both the lambda
-- and function forms and all GROUP BY shapes.
SELECT '-- ROLLUP, lambda grouping ---';
SELECT * APPLY x -> grouping(x) FROM numbers(1) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT '-- ROLLUP, function-form grouping ---';
SELECT * APPLY grouping FROM numbers(1) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT '-- CUBE, lambda grouping ---';
SELECT * APPLY x -> grouping(x) FROM numbers(1) GROUP BY number WITH CUBE ORDER BY 1;

SELECT '-- GROUPING SETS, lambda grouping ---';
SELECT * APPLY x -> grouping(x) FROM numbers(1) GROUP BY GROUPING SETS ((number), ()) ORDER BY 1;

SELECT '-- ROLLUP, grouping nested inside a function ---';
SELECT * APPLY x -> grouping(x) + 1 FROM numbers(1) GROUP BY number WITH ROLLUP ORDER BY 1;

-- Sanity: a non-aggregate APPLY must still produce a `Nullable` projection
-- after `WITH ROLLUP` so the suppression is genuinely scoped to the aggregate
-- argument path and not a blanket disable.
SELECT '-- non-aggregate APPLY remains Nullable ---';
SELECT * APPLY toString FROM (SELECT number FROM numbers(2)) GROUP BY number WITH ROLLUP ORDER BY number;

-- Under group_by_use_nulls, projection resolution (and therefore the SELECT * REPLACE
-- identifier rewrite) is deferred until after WHERE / GROUP BY / HAVING / ORDER BY /
-- LIMIT BY are resolved. Those clauses must still bind a REPLACE'd column to the
-- replacement expression, exactly as with group_by_use_nulls = 0 (#91119). Each pair
-- below must match the equivalent query that writes the replacement as an alias.
SELECT '-- REPLACE reference in ORDER BY under GROUPING SETS ---';
SELECT * REPLACE (-c AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY GROUPING SETS ((), (c)) ORDER BY c;
SELECT '-- REPLACE reference in GROUP BY key under GROUPING SETS ---';
SELECT * REPLACE (intDiv(c, 2) AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY GROUPING SETS ((), (c)) ORDER BY c;
SELECT '-- REPLACE reference in ORDER BY under ROLLUP ---';
SELECT * REPLACE (intDiv(c, 2) AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY c WITH ROLLUP ORDER BY c;
SELECT '-- REPLACE reference in HAVING under GROUPING SETS ---';
SELECT * REPLACE (-c AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY GROUPING SETS ((), (c)) HAVING c < 0 ORDER BY c;
SELECT '-- REPLACE reference in WHERE under GROUPING SETS ---';
SELECT * REPLACE (-c AS c) FROM (SELECT number AS c FROM numbers(4)) WHERE c < 10 GROUP BY GROUPING SETS ((), (c)) ORDER BY c;
SELECT '-- REPLACE reference in ORDER BY under CUBE ---';
SELECT * REPLACE (-c AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY c WITH CUBE ORDER BY c;
SELECT '-- REPLACE with toTypeName in ORDER BY (bot reproducer) ---';
SELECT * REPLACE (toTypeName(c) AS c) FROM (SELECT 1 AS c) GROUP BY GROUPING SETS ((), (1)) ORDER BY c;

-- A non-aggregate APPLY transformer builds its result via resolveFunction /
-- resolveLambda, bypassing the group_by_use_nulls Nullable wrapping that plain
-- columns and REPLACE results receive. When the APPLY result is itself a GROUP BY
-- key (e.g. `* APPLY isNull` with GROUPING SETS keyed on the first projection), it
-- must still be wrapped Nullable so its analyzer type matches the post-rollup
-- runtime type. Otherwise a consuming aggregate bad-casts the runtime Nullable
-- column against the non-Nullable analyzer type (#91119, AST fuzzer STID 1499-2fa0).
SELECT '-- non-aggregate APPLY result as GROUPING SETS key is Nullable ---';
SELECT toTypeName(*) FROM (SELECT * APPLY isNull FROM (SELECT 1::UInt8 AS c) GROUP BY GROUPING SETS ((), (1)));
SELECT '-- minSimpleState over APPLY-result grouping key (crashed before fix) ---';
SELECT minSimpleState(*) FROM (SELECT * APPLY isNull FROM (SELECT 1::UInt8 AS c) GROUP BY GROUPING SETS ((), (1)));
SELECT '-- DISTINCT * APPLY isNull, GROUPING SETS, minSimpleState (fuzzer shape) ---';
SELECT minSimpleState(*) FROM (SELECT DISTINCT * APPLY isNull FROM (SELECT 1::UInt8 AS c) GROUP BY GROUPING SETS ((), (1)));
SELECT '-- lambda-form APPLY result as grouping key is Nullable ---';
SELECT toTypeName(*) FROM (SELECT * APPLY x -> x + 1 FROM (SELECT 1::UInt8 AS c) GROUP BY GROUPING SETS ((), (1)));

-- A named WINDOW definition (`WINDOW w AS (...)`) is resolved after the deferred
-- projection pass, so its `col` references are still unresolved identifiers when the
-- SELECT * REPLACE rewrite runs. The rewrite must reach the window node too, otherwise
-- the deferred group_by_use_nulls path ranks by the original key while the normal path
-- ranks by the replacement (#91119). Each pair below must match.
SELECT '-- REPLACE reference in named WINDOW ORDER BY under GROUPING SETS ---';
SELECT * REPLACE (-c AS c), row_number() OVER w FROM (SELECT number AS c FROM numbers(4)) GROUP BY GROUPING SETS ((), (c)) WINDOW w AS (ORDER BY c) ORDER BY c;
SELECT '-- REPLACE reference in named WINDOW PARTITION BY under GROUPING SETS ---';
SELECT * REPLACE (c % 2 AS c), count() OVER w FROM (SELECT number AS c FROM numbers(6)) GROUP BY GROUPING SETS ((), (c)) WINDOW w AS (PARTITION BY c) ORDER BY c;

-- A function-form APPLY transformer whose transformed expression itself becomes the
-- GROUP BY key: the key shape is `toTypeName(c)`, not bare `c`. The APPLY result must
-- still be wrapped Nullable so a consuming aggregate (minOrNull) does not bad-cast the
-- runtime Nullable(String) column against a non-Nullable analyzer type (#91119).
SELECT '-- function-form APPLY result as grouping key is Nullable ---';
SELECT toTypeName(*) FROM (SELECT * APPLY toTypeName FROM (SELECT 1 AS c) GROUP BY GROUPING SETS ((), (1)));
SELECT '-- minOrNull over function-form APPLY-result grouping key (bad-cast before fix) ---';
SELECT minOrNull(*) FROM (SELECT * APPLY toTypeName FROM (SELECT 1 AS c) GROUP BY GROUPING SETS ((), (1)));

-- The deferred SELECT * REPLACE rewrite must NOT descend into nested scopes
-- (LAMBDA / QUERY / UNION). Those introduce their own name bindings, so a
-- REPLACE name that collides with a lambda argument or a subquery-local column
-- must be left alone there, exactly like the matcher-side rewrite. Rewriting a
-- lambda argument raises `Expected IDENTIFIER or COLUMN as lambda argument`;
-- rewriting a subquery-local identifier silently changes the subquery's result
-- (#91119).
SELECT '-- REPLACE name collides with a lambda argument in WHERE (crashed before fix) ---';
SELECT * REPLACE (1 AS c) FROM (SELECT number AS c FROM numbers(1)) WHERE arrayMap(c -> c + 1, [1]) = [2] GROUP BY GROUPING SETS ((), (c)) ORDER BY c NULLS LAST;
SELECT '-- REPLACE name collides with a lambda argument under ROLLUP (crashed before fix) ---';
SELECT * REPLACE (1 AS c) FROM (SELECT number AS c FROM numbers(2)) WHERE arrayMap(c -> c + 1, [1, 2]) = [2, 3] GROUP BY c WITH ROLLUP ORDER BY c NULLS LAST;
SELECT '-- REPLACE name collides with a subquery-local column (wrong result before fix) ---';
SELECT * REPLACE (1 AS c) FROM (SELECT number AS c FROM numbers(1)) WHERE (SELECT max(c) FROM (SELECT number + 50 AS c FROM numbers(3))) = 52 GROUP BY GROUPING SETS ((), (c)) ORDER BY c NULLS LAST;

-- The deferred SELECT * REPLACE rewrite must be type-selective like the matcher-side
-- rewrite: descend only into function arguments / list elements / a SortNode's sort
-- expression / a window node's ORDER BY|PARTITION BY. In particular it must NOT rewrite
-- a SortNode's WITH FILL FROM/TO/STEP children. A generic getChildren() walk rewrote
-- those, so `ORDER BY ... WITH FILL ... c ...` referencing a REPLACE'd name behaved
-- differently under group_by_use_nulls (making the setting observable outside
-- nullability). Both settings must agree now (#91119).
SELECT '-- WITH FILL bound referencing a REPLACE column: sort expression rewrite is OK, FROM bound is not ---';
SELECT * REPLACE (5 AS c) FROM (SELECT number, 1 AS c FROM numbers(2)) GROUP BY GROUPING SETS ((number), ()) ORDER BY number WITH FILL FROM 0 TO c SETTINGS enable_positional_arguments = 0, group_by_use_nulls = 0; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * REPLACE (5 AS c) FROM (SELECT number, 1 AS c FROM numbers(2)) GROUP BY GROUPING SETS ((number), ()) ORDER BY number WITH FILL FROM 0 TO c SETTINGS enable_positional_arguments = 0, group_by_use_nulls = 1; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT '-- WITH FILL with constant bounds and a REPLACE sort key: both settings agree ---';
SELECT * REPLACE (-c AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY GROUPING SETS ((), (c)) ORDER BY c WITH FILL FROM -5 TO 1 SETTINGS group_by_use_nulls = 0;
SELECT * REPLACE (-c AS c) FROM (SELECT number AS c FROM numbers(4)) GROUP BY GROUPING SETS ((), (c)) ORDER BY c WITH FILL FROM -5 TO 1 SETTINGS group_by_use_nulls = 1;
SELECT '-- mixed lambda tuple(x, sum(x)): the non-aggregate x stays wrapped Nullable per-occurrence ---';
SELECT DISTINCT toTypeName(*) FROM (SELECT * APPLY x -> tuple(x, sum(x)) FROM numbers(2) GROUP BY number WITH ROLLUP) SETTINGS group_by_use_nulls = 1;

-- The deferred SELECT * REPLACE rewrite must build its rewrite map from the columns that
-- actually survive matcher expansion in transformer order, exactly like resolveMatcher. A
-- REPLACE name that an earlier EXCEPT already dropped never reaches the REPLACE step there,
-- so a clause reference to that name stays bound to the grouped source column. Pre-registering
-- every declared REPLACE name would rewrite `HAVING c > 0` to `HAVING 0 > 0` under
-- group_by_use_nulls = 1 and silently drop all rows. Both settings must agree now (#91119).
SELECT '-- EXCEPT c before REPLACE (0 AS c): HAVING keeps c bound to source under ROLLUP ---';
SELECT * EXCEPT c REPLACE (0 AS c) FROM (SELECT number, 1 AS c FROM numbers(3)) GROUP BY number, c WITH ROLLUP HAVING c > 0 ORDER BY number SETTINGS enable_positional_arguments = 0, group_by_use_nulls = 0;
SELECT * EXCEPT c REPLACE (0 AS c) FROM (SELECT number, 1 AS c FROM numbers(3)) GROUP BY number, c WITH ROLLUP HAVING c > 0 ORDER BY number SETTINGS enable_positional_arguments = 0, group_by_use_nulls = 1;
SELECT '-- EXCEPT c before REPLACE (0 AS c): HAVING keeps c bound to source under GROUPING SETS ---';
SELECT * EXCEPT c REPLACE (0 AS c) FROM (SELECT number, 1 AS c FROM numbers(3)) GROUP BY GROUPING SETS ((number, c), ()) HAVING c > 0 ORDER BY number SETTINGS enable_positional_arguments = 0, group_by_use_nulls = 0;
SELECT * EXCEPT c REPLACE (0 AS c) FROM (SELECT number, 1 AS c FROM numbers(3)) GROUP BY GROUPING SETS ((number, c), ()) HAVING c > 0 ORDER BY number SETTINGS enable_positional_arguments = 0, group_by_use_nulls = 1;

-- The deferred SELECT * REPLACE rewrite must build its map only for REPLACE names that a
-- matched SOURCE column actually produces, exactly like resolveMatcher (which registers a
-- rewrite only after findReplacementExpression succeeds for a matched column). A non-strict
-- REPLACE name that matches no produced column is not a replacement target at all: the
-- non-deferred path leaves such a reference unresolved (UNKNOWN_IDENTIFIER). Pre-registering
-- every declared REPLACE name would instead rewrite `HAVING d > 0` to `HAVING 0 > 0` under
-- group_by_use_nulls = 1, silently accepting a query the normal path rejects. Both settings
-- must reject `d` now (#91119).
SELECT '-- REPLACE (0 AS d) with no column d: HAVING d stays unresolved under ROLLUP ---';
SELECT * REPLACE (0 AS d) FROM (SELECT number AS c FROM numbers(3)) GROUP BY c WITH ROLLUP HAVING d > 0 SETTINGS group_by_use_nulls = 0; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * REPLACE (0 AS d) FROM (SELECT number AS c FROM numbers(3)) GROUP BY c WITH ROLLUP HAVING d > 0 SETTINGS group_by_use_nulls = 1; -- { serverError UNKNOWN_IDENTIFIER }
SELECT '-- REPLACE (0 AS d) with no column d: HAVING d stays unresolved under GROUPING SETS ---';
SELECT * REPLACE (0 AS d) FROM (SELECT number AS c FROM numbers(3)) GROUP BY GROUPING SETS ((c), ()) HAVING d > 0 SETTINGS group_by_use_nulls = 0; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * REPLACE (0 AS d) FROM (SELECT number AS c FROM numbers(3)) GROUP BY GROUPING SETS ((c), ()) HAVING d > 0 SETTINGS group_by_use_nulls = 1; -- { serverError UNKNOWN_IDENTIFIER }
SELECT '-- REPLACE (0 AS d) with no column d: ORDER BY d stays unresolved under ROLLUP ---';
SELECT * REPLACE (0 AS d) FROM (SELECT number AS c FROM numbers(3)) GROUP BY c WITH ROLLUP ORDER BY d SETTINGS enable_positional_arguments = 0, group_by_use_nulls = 0; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * REPLACE (0 AS d) FROM (SELECT number AS c FROM numbers(3)) GROUP BY c WITH ROLLUP ORDER BY d SETTINGS enable_positional_arguments = 0, group_by_use_nulls = 1; -- { serverError UNKNOWN_IDENTIFIER }
-- A matching REPLACE name still rewrites the clause reference to the replacement expression
-- under both settings (the map-building change must not drop genuine replacements).
SELECT '-- REPLACE ((c * 10) AS c) matches c: HAVING sees the replacement under both settings ---';
SELECT * REPLACE ((c * 10) AS c) FROM (SELECT number AS c FROM numbers(3)) GROUP BY c WITH ROLLUP HAVING c > 15 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT * REPLACE ((c * 10) AS c) FROM (SELECT number AS c FROM numbers(3)) GROUP BY c WITH ROLLUP HAVING c > 15 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

-- The produced-name set the deferred rewrite gates on must be computed PER MATCHER exactly as
-- resolveMatcher selects source columns: it must respect the matcher qualifier and the
-- asterisk column-kind settings, not a scope-wide union gated only by isMatchingColumn (which
-- ignores the qualifier). Otherwise a qualified matcher `t1.* REPLACE (0 AS c)`, where `c`
-- exists only in another in-scope table, would register `c -> 0` on the deferred path though
-- `t1.*` never produces `c`, silently rewriting `HAVING c > 0` to `HAVING 0 > 0` under
-- group_by_use_nulls = 1 while the non-deferred path keeps `c` bound to `t2.c`. Both settings
-- must agree now (#91119).
SELECT '-- qualified t1.* REPLACE (0 AS c) with c only in t2: HAVING keeps c bound to t2.c under ROLLUP ---';
SELECT count() FROM (SELECT t1.* REPLACE (0 AS c) FROM (SELECT number AS a FROM numbers(3)) t1, (SELECT number AS c FROM numbers(3)) t2 GROUP BY t1.a, t2.c WITH ROLLUP HAVING c > 0 SETTINGS group_by_use_nulls = 0);
SELECT count() FROM (SELECT t1.* REPLACE (0 AS c) FROM (SELECT number AS a FROM numbers(3)) t1, (SELECT number AS c FROM numbers(3)) t2 GROUP BY t1.a, t2.c WITH ROLLUP HAVING c > 0 SETTINGS group_by_use_nulls = 1);

-- The produced-name set must also honor the asterisk column-kind rules: a MATERIALIZED column
-- is hidden from `*` (asterisk_include_materialized_columns = 0 by default), so `* REPLACE (0 AS m)`
-- does not produce `m` and `HAVING m > 0` stays bound to the (non-grouped, non-aggregated)
-- materialized column, which the non-deferred path rejects with NOT_AN_AGGREGATE. A scope-wide
-- name set built from GetColumnsOptions::All would instead register `m -> 0` and silently accept
-- the query under group_by_use_nulls = 1. Both settings must reject it now (#91119).
DROP TABLE IF EXISTS t_04326_matview;
CREATE TABLE t_04326_matview (a UInt64, m UInt64 MATERIALIZED a + 100) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04326_matview (a) VALUES (1), (2), (3);
SELECT '-- * REPLACE (0 AS m) with m MATERIALIZED (hidden from *): HAVING m stays bound to source under ROLLUP ---';
SELECT count() FROM (SELECT * REPLACE (0 AS m) FROM t_04326_matview GROUP BY a WITH ROLLUP HAVING m > 0 SETTINGS group_by_use_nulls = 0); -- { serverError NOT_AN_AGGREGATE }
SELECT count() FROM (SELECT * REPLACE (0 AS m) FROM t_04326_matview GROUP BY a WITH ROLLUP HAVING m > 0 SETTINGS group_by_use_nulls = 1); -- { serverError NOT_AN_AGGREGATE }
DROP TABLE t_04326_matview;
