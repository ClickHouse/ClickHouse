-- reordering these does not change the hash
SELECT normalizedQueryHashUnordered('SELECT a, b FROM t') = normalizedQueryHashUnordered('SELECT b, a FROM t');
SELECT normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY a, b') = normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY b, a');
SELECT normalizedQueryHashUnordered('SELECT a FROM t WHERE x = 1 AND y = 2') = normalizedQueryHashUnordered('SELECT a FROM t WHERE y = 3 AND x = 4');
SELECT normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY GROUPING SETS ((a, b), (c))') = normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY GROUPING SETS ((c), (b, a))');

-- the old hash still cares about the order
SELECT normalizedQueryHash('SELECT a, b FROM t') = normalizedQueryHash('SELECT b, a FROM t');

-- literals erased and literal lists collapsed, as before
SELECT normalizedQueryHashUnordered('SELECT a FROM t WHERE x = 1') = normalizedQueryHashUnordered('SELECT a FROM t WHERE x = 2');
SELECT normalizedQueryHashUnordered('SELECT a FROM t WHERE x IN (1, 2)') = normalizedQueryHashUnordered('SELECT a FROM t WHERE x IN (3, 4, 5)');

-- generated-looking identifiers become a placeholder, as before, and a literal `?` is not one of them
SELECT normalizedQueryHashUnordered('SELECT `a1b2c3` FROM t') = normalizedQueryHashUnordered('SELECT `x9y8z7` FROM t');
SELECT normalizedQueryHashUnordered('SELECT `?` FROM t') = normalizedQueryHashUnordered('SELECT `a1b2c3` FROM t');

-- two elements sharing a placeholder must still sort the same way whichever order they arrive in
SELECT normalizedQueryHashUnordered('SELECT `a1b2c3`, `?` FROM t') = normalizedQueryHashUnordered('SELECT `?`, `a1b2c3` FROM t');
SELECT normalizedQueryHashUnordered('SELECT `a1b2c3`, `x9y8z7` FROM t') = normalizedQueryHashUnordered('SELECT `x9y8z7`, `a1b2c3` FROM t');

-- lossy on purpose: these are not the same query, but every expression list is sorted, so they share a hash
SELECT normalizedQueryHashUnordered('SELECT a FROM t ORDER BY a, b') = normalizedQueryHashUnordered('SELECT a FROM t ORDER BY b, a');
SELECT normalizedQueryHashUnordered('SELECT a - b FROM t') = normalizedQueryHashUnordered('SELECT b - a FROM t');
SELECT normalizedQueryHashUnordered('SELECT (a, b) FROM t') = normalizedQueryHashUnordered('SELECT (b, a) FROM t');
SELECT normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY a, b WITH ROLLUP') = normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY b, a WITH ROLLUP');
SELECT normalizedQueryHashUnordered('SELECT a FROM t1 UNION ALL SELECT b FROM t2') = normalizedQueryHashUnordered('SELECT b FROM t2 UNION ALL SELECT a FROM t1');

-- x -> 1 is lambda(tuple(x), 1), so sorting its arguments loses the same way any other function does
SELECT normalizedQueryHashUnordered('SELECT arrayMap(x -> x + 1, a) FROM t') = normalizedQueryHashUnordered('SELECT arrayMap(x -> x + 2, a) FROM t');
SELECT normalizedQueryHashUnordered('SELECT arrayMap(x -> 1, a) FROM t') = normalizedQueryHashUnordered('SELECT arrayMap(lambda(1, tuple(x)), a) FROM t');
SELECT normalizedQueryHashUnordered('SELECT arrayMap(x -> 1, a) FROM t') = normalizedQueryHashUnordered('SELECT arrayMap(y -> 1, a) FROM t');

-- only what the lexer erases is erased, and every name part is judged on its own
SELECT normalizedQueryHashUnordered('SELECT NULL FROM t') = normalizedQueryHashUnordered('SELECT 1 FROM t');
SELECT normalizedQueryHashUnordered('SELECT true FROM t') = normalizedQueryHashUnordered('SELECT 1 FROM t');
SELECT normalizedQueryHashUnordered('SELECT db1.t34 FROM t') = normalizedQueryHashUnordered('SELECT db1.t56 FROM t');
SELECT normalizedQueryHashUnordered('SELECT 1 AS x FROM t') = normalizedQueryHashUnordered('SELECT 1 AS y FROM t');

-- state that lives outside the AST children is still part of the query
SELECT normalizedQueryHashUnordered('SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t') = normalizedQueryHashUnordered('SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t');
SELECT normalizedQueryHashUnordered('SELECT sum(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t') = normalizedQueryHashUnordered('SELECT sum(x) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t');
SELECT normalizedQueryHashUnordered('WITH c AS MATERIALIZED (SELECT 1) SELECT * FROM c') = normalizedQueryHashUnordered('WITH c AS (SELECT 1) SELECT * FROM c');
SELECT normalizedQueryHashUnordered('SELECT 1 UNION ALL SELECT 2') = normalizedQueryHashUnordered('SELECT 1 UNION DISTINCT SELECT 2');
SELECT normalizedQueryHashUnordered('SELECT 1 INTERSECT SELECT 2') = normalizedQueryHashUnordered('SELECT 1 EXCEPT SELECT 2');

-- not everything collapses
SELECT normalizedQueryHashUnordered('SELECT a, b FROM t') = normalizedQueryHashUnordered('SELECT a, c FROM t');
SELECT normalizedQueryHashUnordered('SELECT a FROM t') = normalizedQueryHashUnordered('SELECT a FROM u');

-- unparseable input
SELECT normalizedQueryHashUnordered('SELECT * FROM'); -- { serverError SYNTAX_ERROR }
SELECT normalizedQueryHashUnorderedOrNull('SELECT * FROM');
SELECT normalizedQueryHashUnorderedOrNull('SELECT a, b FROM t') = normalizedQueryHashUnorderedOrNull('SELECT b, a FROM t');

-- parser knobs come from the session: with the setting off, SETTINGS after FORMAT is data rather than a clause
SELECT normalizedQueryHashUnorderedOrNull('INSERT INTO t FORMAT CSV SETTINGS max_threads = 1') = normalizedQueryHashUnorderedOrNull('INSERT INTO t FORMAT CSV') SETTINGS allow_settings_after_format_in_insert = 0;
SELECT normalizedQueryHashUnorderedOrNull('INSERT INTO t FORMAT CSV SETTINGS max_threads = 1') = normalizedQueryHashUnorderedOrNull('INSERT INTO t FORMAT CSV') SETTINGS allow_settings_after_format_in_insert = 1;

-- hitting either half of max_parser_depth is unparseable too, the parser recursion and the finished AST
SELECT normalizedQueryHashUnorderedOrNull('SELECT ' || repeat('(', 60) || '1' || repeat(')', 60)) IS NULL SETTINGS max_parser_depth = 40;
SELECT normalizedQueryHashUnorderedOrNull('SELECT ' || repeat('1+', 199) || '1') IS NULL SETTINGS max_parser_depth = 20;

-- sub-expressions the parser keeps outside the AST children are left in the order they were written
SELECT normalizedQueryHashUnordered('SELECT * APPLY (x -> plus(x, 1)) FROM t') = normalizedQueryHashUnordered('SELECT * APPLY (x -> plus(1, x)) FROM t');

-- over a column, not just constants
SELECT uniqExact(normalizedQueryHashUnorderedOrNull(q))
FROM values('q String', 'SELECT a, b FROM t', 'SELECT b, a FROM t', 'SELECT a FROM t', 'SELECT * FROM');

-- same type contract as normalizedQueryHash on Dynamic and Variant
SELECT toTypeName(normalizedQueryHashUnordered(d)) = toTypeName(normalizedQueryHash(d)) FROM (SELECT 'SELECT a'::Dynamic AS d);
SELECT normalizedQueryHashUnordered('SELECT a'::Variant(String, UInt64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
