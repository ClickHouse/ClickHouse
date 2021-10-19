SELECT 'Array';

SELECT toTypeName([toDate('2000-01-01'), toDateTime('2000-01-01', 'Europe/Moscow')]);
SELECT toTypeName([toDate('2000-01-01'), toDateTime('2000-01-01', 'Europe/Moscow'), toDateTime64('2000-01-01', 5, 'Europe/Moscow')]);
SELECT toTypeName([toDate('2000-01-01'), toDateTime('2000-01-01', 'Europe/Moscow'), toDateTime64('2000-01-01', 5, 'Europe/Moscow'), toDateTime64('2000-01-01', 6, 'Europe/Moscow')]);

DROP TABLE IF EXISTS predicate_table;
CREATE TABLE predicate_table (value UInt8) ENGINE=TinyLog;

INSERT INTO predicate_table VALUES (0), (1);

SELECT 'If';

WITH toDate('2000-01-01') as a, toDateTime('2000-01-01', 'Europe/Moscow') as b
SELECT if(value, b, a) as result, toTypeName(result)
FROM predicate_table;

WITH toDateTime('2000-01-01', 'Europe/Moscow') as a, toDateTime64('2000-01-01', 5, 'Europe/Moscow') as b
SELECT if(value, b, a) as result, toTypeName(result)
FROM predicate_table;

SELECT 'Cast';
SELECT CAST(toDate('2000-01-01') AS DateTime('UTC')) AS x, toTypeName(x);
SELECT CAST(toDate('2000-01-01') AS DateTime64(5, 'UTC')) AS x, toTypeName(x);
