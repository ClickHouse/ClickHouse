-- The `VALUES` carrier converts a `Time` / `Time64` constant to `Date` through `convertFieldToType`,
-- which must honor `date_time_overflow_behavior` exactly like the column path used by `CAST` and
-- `INSERT SELECT`.

-- With asynchronous inserts the inline `VALUES` data is parsed on the server, otherwise on the
-- client; pin the setting so the expected error kind below is deterministic.
SET async_insert = 0;

DROP TABLE IF EXISTS t_time64_date_overflow;
CREATE TABLE t_time64_date_overflow (d Date) ENGINE = Memory;

SELECT 'saturate';
SET date_time_overflow_behavior = 'saturate';
INSERT INTO t_time64_date_overflow VALUES (CAST(-1 AS Time64(0))), (CAST(-1 AS Time));
INSERT INTO t_time64_date_overflow SELECT CAST(-1 AS Time64(0));
SELECT DISTINCT d FROM t_time64_date_overflow;
SELECT CAST(CAST(-1 AS Time64(0)) AS Date), CAST(CAST(-1 AS Time) AS Date);

TRUNCATE TABLE t_time64_date_overflow;

SELECT 'throw';
SET date_time_overflow_behavior = 'throw';
-- `clickhouse-client` parses inline `VALUES` data on the client side, so the exception is a client one.
INSERT INTO t_time64_date_overflow VALUES (CAST(-1 AS Time64(0))); -- { clientError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_time64_date_overflow VALUES (CAST(-1 AS Time)); -- { clientError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(CAST(-1 AS Time64(0)) AS Date); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT count() FROM t_time64_date_overflow;

SELECT 'ignore';
SET date_time_overflow_behavior = 'ignore';
INSERT INTO t_time64_date_overflow VALUES (CAST(-1 AS Time64(0))), (CAST(-1 AS Time));
SELECT DISTINCT d FROM t_time64_date_overflow;
SELECT CAST(CAST(-1 AS Time64(0)) AS Date), CAST(CAST(-1 AS Time) AS Date);

DROP TABLE t_time64_date_overflow;
