-- a unary sign, NULL, TRUE and FALSE normalize like any other literal
SELECT a, b,
    normalizeQuery(a) = normalizeQuery(b) AS text,
    normalizedQueryHash(a) = normalizedQueryHash(b) AS hash,
    normalizedQueryHashUnordered(a) = normalizedQueryHashUnordered(b) AS unordered
FROM values('a String, b String',
    ('SELECT 1', 'SELECT -1'),
    ('SELECT x FROM t WHERE a > 1', 'SELECT x FROM t WHERE a > -1'),
    ('SELECT x FROM t WHERE a IN (1, 2)', 'SELECT x FROM t WHERE a IN (1, -2)'),
    ('SELECT x FROM t WHERE a IN (1, 2)', 'SELECT x FROM t WHERE a IN (-1, +2)'),
    ('SELECT f(1, 2)', 'SELECT f(-1, - 2)'),
    ('SELECT x FROM t WHERE a = 1 AND b = 2', 'SELECT x FROM t WHERE a = -1 AND b = -2'),
    ('SELECT CASE WHEN a THEN 1 ELSE 2 END', 'SELECT CASE WHEN a THEN -1 ELSE -2 END'),
    ('SELECT x FROM t WHERE a = 1', 'SELECT x FROM t WHERE a = NULL'),
    ('SELECT x FROM t WHERE a IN (1, 2)', 'SELECT x FROM t WHERE a IN (1, null)'),
    ('SELECT x FROM t WHERE a = 1', 'SELECT x FROM t WHERE a = true'),
    ('INSERT INTO t VALUES (1, 2)', 'INSERT INTO t VALUES (FALSE, NULL)'))
FORMAT TSV;

-- a binary minus stays
SELECT a, b,
    normalizeQuery(a) = normalizeQuery(b) AS text,
    normalizedQueryHash(a) = normalizedQueryHash(b) AS hash,
    normalizedQueryHashUnordered(a) = normalizedQueryHashUnordered(b) AS unordered
FROM values('a String, b String',
    ('SELECT a - 1 FROM t', 'SELECT a + 1 FROM t'),
    ('SELECT a - 1 FROM t', 'SELECT a 1 FROM t'),
    ('SELECT x - y FROM t', 'SELECT x y FROM t'))
FORMAT TSV;

SELECT normalizeQuery(q) FROM values('q String',
    'SELECT -1',
    'SELECT x FROM t WHERE a > -1 AND b IN (-1, 2, NULL)',
    'SELECT x FROM t WHERE a > - 1',
    'SELECT a - 1 FROM t',
    'SELECT x FROM t WHERE a IS NOT NULL');
