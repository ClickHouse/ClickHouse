SELECT * FROM ((SELECT * FROM system.numbers LIMIT 1) UNION ALL SELECT * FROM system.numbers LIMIT 2 UNION ALL (SELECT * FROM system.numbers LIMIT 3)) ORDER BY number;
SELECT * FROM (SELECT * FROM system.numbers LIMIT 1 UNION ALL (SELECT * FROM system.numbers LIMIT 2 UNION ALL (SELECT * FROM system.numbers LIMIT 3))) ORDER BY number;
