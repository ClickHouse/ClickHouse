-- https://github.com/ClickHouse/ClickHouse/issues/51321
EXPLAIN ESTIMATE  SELECT any(toTypeName(s)) FROM (SELECT 'bbbbbbbb', toTypeName(s), CAST('', 'LowCardinality(String)'), NULL, CAST('\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0', 'String') AS s) AS t1 FULL OUTER JOIN (SELECT CAST('bbbbb\0\0bbb\0bb\0bb', 'LowCardinality(String)'), CAST(CAST('a', 'String'), 'LowCardinality(String)') AS s GROUP BY CoNnEcTiOn_Id()) AS t2 USING (s) WITH TOTALS;
EXPLAIN ESTIMATE SELECT any(s) FROM (SELECT '' AS s) AS t1 JOIN (SELECT '' AS s GROUP BY connection_id()) AS t2 USING (s);
