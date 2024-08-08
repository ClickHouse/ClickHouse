WITH 'SELECT tupleElement((\'a\', 1, 10) AS x, 1) = \'a\'' AS q SELECT formatQuery(q), formatQuery(q) = formatQuery(formatQuery(q));
SELECT tupleElement(('a', 1, 10) AS x, 1) = 'a';
SELECT ((tuple('a', 1, 10) AS x).1) = 'a';
