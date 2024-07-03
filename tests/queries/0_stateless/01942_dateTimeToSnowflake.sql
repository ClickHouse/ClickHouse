-- Error cases
SELECT dateTimeToSnowflake();  -- {serverError 42}
SELECT dateTime64ToSnowflake();  -- {serverError 42}

SELECT dateTimeToSnowflake('abc');  -- {serverError 43}
SELECT dateTime64ToSnowflake('abc');  -- {serverError 43}

SELECT dateTimeToSnowflake('abc', 123);  -- {serverError 42}
SELECT dateTime64ToSnowflake('abc', 123);  -- {serverError 42}

SELECT 'const column';
WITH toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai') AS dt
SELECT dt, dateTimeToSnowflake(dt);

WITH toDateTime64('2021-08-15 18:57:56.492', 3, 'Asia/Shanghai') AS dt64
SELECT dt64, dateTime64ToSnowflake(dt64);

SELECT 'non-const column';
WITH toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai') AS x
SELECT materialize(x) as dt, dateTimeToSnowflake(dt);;

WITH toDateTime64('2021-08-15 18:57:56.492', 3, 'Asia/Shanghai') AS x
SELECT materialize(x) as dt64, dateTime64ToSnowflake(dt64);
