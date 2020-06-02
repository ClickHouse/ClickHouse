-- Error cases
SELECT toStartOfSecond('123');  -- {serverError 43}
SELECT toStartOfSecond(now());  -- {serverError 43}
SELECT toStartOfSecond();   -- {serverError 42}
SELECT toStartOfSecond(now64(), 123);   -- {serverError 43}

WITH toDateTime64('2019-09-16 19:20:11', 3) AS dt64 SELECT toStartOfSecond(dt64, 'UTC') AS res, toTypeName(res);
WITH toDateTime64('2019-09-16 19:20:11', 0, 'UTC') AS dt64 SELECT toStartOfSecond(dt64) AS res, toTypeName(res);
WITH toDateTime64('2019-09-16 19:20:11.123', 3, 'UTC') AS dt64 SELECT toStartOfSecond(dt64) AS res, toTypeName(res);
WITH toDateTime64('2019-09-16 19:20:11.123', 9, 'UTC') AS dt64 SELECT toStartOfSecond(dt64) AS res, toTypeName(res);

SELECT 'non-const column';
WITH toDateTime64('2019-09-16 19:20:11.123', 3, 'UTC') AS dt64 SELECT toStartOfSecond(materialize(dt64)) AS res, toTypeName(res);