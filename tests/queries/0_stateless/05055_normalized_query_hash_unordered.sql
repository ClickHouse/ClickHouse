-- reordering these does not change the hash
SELECT normalizedQueryHashUnordered('SELECT a, b FROM t') = normalizedQueryHashUnordered('SELECT b, a FROM t');
SELECT normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY a, b') = normalizedQueryHashUnordered('SELECT count() FROM t GROUP BY b, a');
SELECT normalizedQueryHashUnordered('SELECT a FROM t WHERE x = 1 AND y = 2') = normalizedQueryHashUnordered('SELECT a FROM t WHERE y = 3 AND x = 4');

-- the old hash still cares about the order
SELECT normalizedQueryHash('SELECT a, b FROM t') = normalizedQueryHash('SELECT b, a FROM t');

-- literals erased and literal lists collapsed, as in normalizedQueryHash
SELECT normalizedQueryHashUnordered('SELECT a FROM t WHERE x = 1') = normalizedQueryHashUnordered('SELECT a FROM t WHERE x = 2');
SELECT normalizedQueryHashUnordered('SELECT a FROM t WHERE x IN (1, 2)') = normalizedQueryHashUnordered('SELECT a FROM t WHERE x IN (3, 4, 5)');
SELECT normalizedQueryHashUnordered('SELECT a FROM t WHERE x IN (1)') = normalizedQueryHashUnordered('SELECT a FROM t WHERE x IN (1, 2)');

-- generated-looking identifiers become a placeholder, and a literal `?` is not one of them
SELECT normalizedQueryHashUnordered('SELECT `a1b2c3` FROM t') = normalizedQueryHashUnordered('SELECT `x9y8z7` FROM t');
SELECT normalizedQueryHashUnordered('SELECT `?` FROM t') = normalizedQueryHashUnordered('SELECT `a1b2c3` FROM t');

-- only tokens are compared, so any reordering of the same tokens collapses, meaningful or not
SELECT normalizedQueryHashUnordered('SELECT a FROM t ORDER BY a, b') = normalizedQueryHashUnordered('SELECT a FROM t ORDER BY b, a');
SELECT normalizedQueryHashUnordered('SELECT a - b FROM t') = normalizedQueryHashUnordered('SELECT b - a FROM t');
SELECT normalizedQueryHashUnordered('SELECT a FROM t1 UNION ALL SELECT b FROM t2') = normalizedQueryHashUnordered('SELECT b FROM t2 UNION ALL SELECT a FROM t1');
SELECT normalizedQueryHashUnordered('SELECT 1') = normalizedQueryHashUnordered('1 SELECT');

-- a token repeated is not the same as a token once, so it is a sum and not xor
SELECT normalizedQueryHashUnordered('SELECT a, a FROM t') = normalizedQueryHashUnordered('SELECT a FROM t');

-- different tokens still differ
SELECT normalizedQueryHashUnordered('SELECT a, b FROM t') = normalizedQueryHashUnordered('SELECT a, c FROM t');
SELECT normalizedQueryHashUnordered('SELECT NULL FROM t') = normalizedQueryHashUnordered('SELECT 1 FROM t');
SELECT normalizedQueryHashUnordered('SELECT 1 AS x FROM t') = normalizedQueryHashUnordered('SELECT 1 AS y FROM t');
SELECT normalizedQueryHashUnordered('SELECT 1 UNION ALL SELECT 2') = normalizedQueryHashUnordered('SELECT 1 UNION DISTINCT SELECT 2');
SELECT normalizedQueryHashUnordered('SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t') = normalizedQueryHashUnordered('SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t');
SELECT normalizedQueryHashUnordered('INSERT INTO t VALUES (1)') = normalizedQueryHashUnordered('INSERT INTO t VALUES (1, 2)');

-- nothing is parsed, so malformed text is hashed rather than rejected
SELECT normalizedQueryHashUnordered('SELECT * FROM') = normalizedQueryHashUnordered('FROM * SELECT');
SELECT normalizedQueryHashUnordered('SELECT \'unclosed') = normalizedQueryHashUnordered('SELECT \'closed\'');

-- over a column, not just constants
SELECT uniqExact(normalizedQueryHashUnordered(q))
FROM values('q String', 'SELECT a, b FROM t', 'SELECT b, a FROM t', 'SELECT a FROM t', 'SELECT * FROM');

-- same type contract as normalizedQueryHash on Dynamic and Variant
SELECT toTypeName(normalizedQueryHashUnordered(d)) = toTypeName(normalizedQueryHash(d)) FROM (SELECT 'SELECT a'::Dynamic AS d);
SELECT normalizedQueryHashUnordered('SELECT a'::Variant(String, UInt64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
