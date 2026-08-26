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

-- generated-looking identifiers become a placeholder, as before
SELECT normalizedQueryHashUnordered('SELECT `a1b2c3` FROM t') = normalizedQueryHashUnordered('SELECT `x9y8z7` FROM t');

-- lossy on purpose: these are not the same query, but every expression list is sorted, so they share a hash
SELECT normalizedQueryHashUnordered('SELECT a FROM t ORDER BY a, b') = normalizedQueryHashUnordered('SELECT a FROM t ORDER BY b, a');
SELECT normalizedQueryHashUnordered('SELECT a - b FROM t') = normalizedQueryHashUnordered('SELECT b - a FROM t');
SELECT normalizedQueryHashUnordered('SELECT (a, b) FROM t') = normalizedQueryHashUnordered('SELECT (b, a) FROM t');
SELECT normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY a, b WITH ROLLUP') = normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY b, a WITH ROLLUP');
SELECT normalizedQueryHashUnordered('SELECT a FROM t1 UNION ALL SELECT b FROM t2') = normalizedQueryHashUnordered('SELECT b FROM t2 UNION ALL SELECT a FROM t1');

-- a lambda is hashed from the AST, so its rendering plays no part
SELECT normalizedQueryHashUnordered('SELECT arrayMap(x -> x + 1, a) FROM t') = normalizedQueryHashUnordered('SELECT arrayMap(x -> x + 2, a) FROM t');

-- only what the lexer erases is erased, and every name part is judged on its own
SELECT normalizedQueryHashUnordered('SELECT NULL FROM t') = normalizedQueryHashUnordered('SELECT 1 FROM t');
SELECT normalizedQueryHashUnordered('SELECT true FROM t') = normalizedQueryHashUnordered('SELECT 1 FROM t');
SELECT normalizedQueryHashUnordered('SELECT db1.t34 FROM t') = normalizedQueryHashUnordered('SELECT db1.t56 FROM t');
SELECT normalizedQueryHashUnordered('SELECT 1 AS x FROM t') = normalizedQueryHashUnordered('SELECT 1 AS y FROM t');

-- the set operator is part of the query, even though it does not live in the AST children
SELECT normalizedQueryHashUnordered('SELECT 1 UNION ALL SELECT 2') = normalizedQueryHashUnordered('SELECT 1 UNION DISTINCT SELECT 2');
SELECT normalizedQueryHashUnordered('SELECT 1 INTERSECT SELECT 2') = normalizedQueryHashUnordered('SELECT 1 EXCEPT SELECT 2');

-- not everything collapses
SELECT normalizedQueryHashUnordered('SELECT a, b FROM t') = normalizedQueryHashUnordered('SELECT a, c FROM t');
SELECT normalizedQueryHashUnordered('SELECT a FROM t') = normalizedQueryHashUnordered('SELECT a FROM u');

-- unparseable input
SELECT normalizedQueryHashUnordered('SELECT * FROM'); -- { serverError SYNTAX_ERROR }
SELECT normalizedQueryHashUnorderedOrNull('SELECT * FROM');
SELECT normalizedQueryHashUnorderedOrNull('SELECT a, b FROM t') = normalizedQueryHashUnorderedOrNull('SELECT b, a FROM t');

-- hitting a parser limit is unparseable too
SELECT normalizedQueryHashUnorderedOrNull('SELECT ' || repeat('(', 60) || '1' || repeat(')', 60)) IS NULL SETTINGS max_parser_depth = 40;

-- over a column, not just constants
SELECT uniqExact(normalizedQueryHashUnorderedOrNull(q))
FROM values('q String', 'SELECT a, b FROM t', 'SELECT b, a FROM t', 'SELECT a FROM t', 'SELECT * FROM');

-- same type contract as normalizedQueryHash on Dynamic and Variant
SELECT toTypeName(normalizedQueryHashUnordered(d)) = toTypeName(normalizedQueryHash(d)) FROM (SELECT 'SELECT a'::Dynamic AS d);
SELECT normalizedQueryHashUnordered('SELECT a'::Variant(String, UInt64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
