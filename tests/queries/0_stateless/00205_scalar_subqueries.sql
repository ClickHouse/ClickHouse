SET send_logs_level = 'fatal';

SELECT (SELECT (SELECT (SELECT (SELECT (SELECT count() FROM (SELECT * FROM system.numbers LIMIT 10)))))) = (SELECT 10), ((SELECT 1, 'Hello', [1, 2]).3)[1];
SELECT toUInt64((SELECT 9)) IN (SELECT number FROM system.numbers LIMIT 10);
SELECT (SELECT toDate('2015-01-02')) = toDate('2015-01-02'), 'Hello' = (SELECT 'Hello');
SELECT (SELECT toDate('2015-01-02'), 'Hello');
SELECT (SELECT toDate('2015-01-02'), 'Hello') AS x, x, identity((SELECT 1)), identity((SELECT 1) AS y);
-- SELECT (SELECT uniqState(''));

 SELECT ( SELECT throwIf(1 + dummy) );  -- { serverError 395 }

-- Scalar subquery with 0 rows must return Null
SELECT (SELECT 1 WHERE 0);
-- But tuple and array can't be inside nullable
SELECT (SELECT 1, 2 WHERE 0); -- { serverError 125 }
SELECT (SELECT [1] WHERE 0); -- { serverError 125 }
-- Works for not-empty casle
SELECT (SELECT 1, 2);
SELECT (SELECT [1]);
-- Several rows
SELECT (SELECT number FROM numbers(2)); -- { serverError 125 }
