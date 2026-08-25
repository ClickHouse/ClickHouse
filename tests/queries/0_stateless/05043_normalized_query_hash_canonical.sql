-- Reordered SELECT expressions, GROUP BY keys and AND/OR operands hash the same.
SELECT normalizedQueryHashCanonical('SELECT a, b FROM t') = normalizedQueryHashCanonical('SELECT b, a FROM t');
SELECT normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY a, b') = normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY b, a');
SELECT normalizedQueryHashCanonical('SELECT a FROM t WHERE x = 1 AND y = 2') = normalizedQueryHashCanonical('SELECT a FROM t WHERE y = 3 AND x = 4');
SELECT normalizedQueryHashCanonical('SELECT a FROM t WHERE x = 1 OR y = 2') = normalizedQueryHashCanonical('SELECT a FROM t WHERE y = 2 OR x = 1');

-- The old hash is order sensitive and stays that way.
SELECT normalizedQueryHash('SELECT a, b FROM t') = normalizedQueryHash('SELECT b, a FROM t');

-- Literals are erased and lists of literals are collapsed, like in normalizedQueryHash.
SELECT normalizedQueryHashCanonical('SELECT a FROM t WHERE x = 1') = normalizedQueryHashCanonical('SELECT a FROM t WHERE x = 2');
SELECT normalizedQueryHashCanonical('SELECT a FROM t WHERE x IN (1, 2)') = normalizedQueryHashCanonical('SELECT a FROM t WHERE x IN (3, 4, 5)');

-- Complex identifiers are replaced by a placeholder, like in normalizedQueryHash.
SELECT normalizedQueryHashCanonical('SELECT `a1b2c3` FROM t') = normalizedQueryHashCanonical('SELECT `x9y8z7` FROM t');

-- Order stays significant where it changes the result.
SELECT normalizedQueryHashCanonical('SELECT a FROM t ORDER BY a, b') = normalizedQueryHashCanonical('SELECT a FROM t ORDER BY b, a');
SELECT normalizedQueryHashCanonical('SELECT a FROM t LIMIT 1 BY a, b') = normalizedQueryHashCanonical('SELECT a FROM t LIMIT 1 BY b, a');
SELECT normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY a, b WITH ROLLUP') = normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY b, a WITH ROLLUP');
SELECT normalizedQueryHashCanonical('SELECT a - b FROM t') = normalizedQueryHashCanonical('SELECT b - a FROM t');
SELECT normalizedQueryHashCanonical('SELECT (a, b) FROM t') = normalizedQueryHashCanonical('SELECT (b, a) FROM t');
SELECT normalizedQueryHashCanonical('SELECT a FROM t1 UNION ALL SELECT b FROM t2') = normalizedQueryHashCanonical('SELECT b FROM t2 UNION ALL SELECT a FROM t1');

-- Different queries still differ.
SELECT normalizedQueryHashCanonical('SELECT a, b FROM t') = normalizedQueryHashCanonical('SELECT a, c FROM t');

-- CUBE and GROUPING SETS do not depend on the order of the keys.
SELECT normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY a, b WITH CUBE') = normalizedQueryHashCanonical('SELECT count() FROM t GROUP BY b, a WITH CUBE');

-- Parse errors: throws, or returns NULL for the OrNull variant.
SELECT normalizedQueryHashCanonical('SELECT * FROM'); -- { serverError SYNTAX_ERROR }
SELECT normalizedQueryHashCanonicalOrNull('SELECT * FROM');
SELECT normalizedQueryHashCanonicalOrNull('SELECT a, b FROM t') = normalizedQueryHashCanonicalOrNull('SELECT b, a FROM t');

-- Works over a column, not only over constants.
SELECT uniqExact(normalizedQueryHashCanonicalOrNull(q))
FROM values('q String', 'SELECT a, b FROM t', 'SELECT b, a FROM t', 'SELECT a FROM t', 'SELECT * FROM');
