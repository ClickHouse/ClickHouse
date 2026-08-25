-- reordering these does not change the hash
SELECT normalizedQueryHashCanonical('SELECT a, b FROM t') = normalizedQueryHashCanonical('SELECT b, a FROM t');
SELECT normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY a, b') = normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY b, a');
SELECT normalizedQueryHashCanonical('SELECT a FROM t WHERE x = 1 AND y = 2') = normalizedQueryHashCanonical('SELECT a FROM t WHERE y = 3 AND x = 4');
SELECT normalizedQueryHashCanonical('SELECT a FROM t WHERE x = 1 OR y = 2') = normalizedQueryHashCanonical('SELECT a FROM t WHERE y = 2 OR x = 1');

-- the canonical text, and the hash is just normalizedQueryHash of it
SELECT normalizeQueryCanonical('SELECT b, a FROM t WHERE y = 2 AND x = 1');
SELECT normalizedQueryHashCanonical('SELECT b, a FROM t WHERE y = 2 AND x = 1') = normalizedQueryHash(normalizeQueryCanonical('SELECT b, a FROM t WHERE y = 2 AND x = 1'));

-- the old hash still cares about the order
SELECT normalizedQueryHash('SELECT a, b FROM t') = normalizedQueryHash('SELECT b, a FROM t');

-- literals erased and literal lists collapsed, as before
SELECT normalizedQueryHashCanonical('SELECT a FROM t WHERE x = 1') = normalizedQueryHashCanonical('SELECT a FROM t WHERE x = 2');
SELECT normalizedQueryHashCanonical('SELECT a FROM t WHERE x IN (1, 2)') = normalizedQueryHashCanonical('SELECT a FROM t WHERE x IN (3, 4, 5)');

-- generated-looking identifiers become a placeholder, as before
SELECT normalizedQueryHashCanonical('SELECT `a1b2c3` FROM t') = normalizedQueryHashCanonical('SELECT `x9y8z7` FROM t');

-- here the order matters, so the hashes differ
SELECT normalizedQueryHashCanonical('SELECT a FROM t ORDER BY a, b') = normalizedQueryHashCanonical('SELECT a FROM t ORDER BY b, a');
SELECT normalizedQueryHashCanonical('SELECT a FROM t LIMIT 1 BY a, b') = normalizedQueryHashCanonical('SELECT a FROM t LIMIT 1 BY b, a');
SELECT normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY a, b WITH ROLLUP') = normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY b, a WITH ROLLUP');
SELECT normalizedQueryHashCanonical('SELECT a - b FROM t') = normalizedQueryHashCanonical('SELECT b - a FROM t');
SELECT normalizedQueryHashCanonical('SELECT (a, b) FROM t') = normalizedQueryHashCanonical('SELECT (b, a) FROM t');
SELECT normalizedQueryHashCanonical('SELECT a FROM t1 UNION ALL SELECT b FROM t2') = normalizedQueryHashCanonical('SELECT b FROM t2 UNION ALL SELECT a FROM t1');

-- not everything collapses
SELECT normalizedQueryHashCanonical('SELECT a, b FROM t') = normalizedQueryHashCanonical('SELECT a, c FROM t');

-- CUBE does not care about the key order
SELECT normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY a, b WITH CUBE') = normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY b, a WITH CUBE');

-- neither the sets nor the keys inside them are ordered
SELECT normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY GROUPING SETS ((a, b), (c))') = normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY GROUPING SETS ((c), (b, a))');
SELECT normalizeQueryCanonical('SELECT count() FROM t GROUP BY GROUPING SETS ((c), (b, a))');

-- unparseable input
SELECT normalizedQueryHashCanonical('SELECT * FROM'); -- { serverError SYNTAX_ERROR }
SELECT normalizedQueryHashCanonicalOrNull('SELECT * FROM');
SELECT normalizeQueryCanonicalOrNull('SELECT * FROM');

-- hitting a parser limit is unparseable too
SELECT normalizeQueryCanonicalOrNull('SELECT ' || repeat('(', 60) || '1' || repeat(')', 60)) IS NULL SETTINGS max_parser_depth = 40;
SELECT normalizedQueryHashCanonicalOrNull('SELECT a, b FROM t') = normalizedQueryHashCanonicalOrNull('SELECT b, a FROM t');

-- over a column, not just constants
SELECT uniqExact(normalizedQueryHashCanonicalOrNull(q))
FROM values('q String', 'SELECT a, b FROM t', 'SELECT b, a FROM t', 'SELECT a FROM t', 'SELECT * FROM');

-- same type contract as the plain functions on Dynamic and Variant
SELECT toTypeName(normalizeQueryCanonical(d)) = toTypeName(normalizeQuery(d)), toTypeName(normalizedQueryHashCanonical(d)) = toTypeName(normalizedQueryHash(d)) FROM (SELECT 'SELECT a'::Dynamic AS d);
SELECT toTypeName(normalizeQueryCanonical(v)) = toTypeName(normalizeQuery(v)) FROM (SELECT 'SELECT a'::Variant(String, UInt64) AS v);
SELECT normalizedQueryHashCanonical('SELECT a'::Variant(String, UInt64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
