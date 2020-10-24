SELECT * FROM numbers(10) UNION SELECT * FROM numbers(10);
SELECT * FROM numbers(10) UNION ALL SELECT * FROM numbers(10);
SELECT * FROM numbers(10) UNION DISTINCT SELECT * FROM numbers(10);

SET union_default_mode='ALL';
SELECT * FROM numbers(10) UNION SELECT * FROM numbers(10);
SELECT * FROM numbers(10) UNION ALL SELECT * FROM numbers(10);
SELECT * FROM numbers(10) UNION DISTINCT SELECT * FROM numbers(10);

SET union_default_mode='DISTINCT';
SELECT * FROM numbers(10) UNION SELECT * FROM numbers(10);
SELECT * FROM numbers(10) UNION ALL SELECT * FROM numbers(10);
SELECT * FROM numbers(10) UNION DISTINCT SELECT * FROM numbers(10);
