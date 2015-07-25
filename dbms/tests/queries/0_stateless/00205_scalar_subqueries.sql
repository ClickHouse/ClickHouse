SELECT (SELECT (SELECT (SELECT (SELECT (SELECT count() FROM (SELECT * FROM system.numbers LIMIT 10)))))) = (SELECT 10), ((SELECT 1, 'Hello', [1, 2]).3)[1];
