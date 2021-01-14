SET send_logs_level = 'fatal';

SELECT (SELECT (SELECT (SELECT (SELECT (SELECT count() FROM (SELECT * FROM system.numbers LIMIT 10)))))) = (SELECT 10), ((SELECT 1, 'Hello', [1, 2]).3)[1];
SELECT toUInt64((SELECT 9)) IN (SELECT number FROM system.numbers LIMIT 10);
SELECT (SELECT toDate('2015-01-02')) = toDate('2015-01-02'), 'Hello' = (SELECT 'Hello');
SELECT (SELECT toDate('2015-01-02'), 'Hello');
SELECT (SELECT toDate('2015-01-02'), 'Hello') AS x, x, identity((SELECT 1)), identity((SELECT 1) AS y);
-- SELECT (SELECT uniqState(''));

