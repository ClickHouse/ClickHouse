-- `convertFieldToType` coerces a `Date32` literal to `DateTime` when it is used as an exact bound - in a
-- set for `IN`, in `KeyCondition`, or in `INSERT ... VALUES`. It used to narrow the day number to `UInt16`,
-- which wraps around for the extended range: `0000-01-01` is day `-719528` and became day `1368`, so the
-- bound spuriously matched rows on `1973-09-30`. The exact timestamp is used instead, and a timestamp
-- outside the range of `DateTime` matches nothing.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_date32_coercion;
CREATE TABLE t_date32_coercion (d DateTime) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_date32_coercion VALUES ('1970-01-01 00:00:00'), ('1973-09-30 00:00:00'), ('2024-01-01 00:00:00');

SELECT count() FROM t_date32_coercion WHERE d IN (toDate32('0000-01-01'));
SELECT count() FROM t_date32_coercion WHERE d IN (toDate32('1899-12-31'));
SELECT count() FROM t_date32_coercion WHERE d IN (toDate32('9999-12-31'));
SELECT count() FROM t_date32_coercion WHERE d IN (toDate32('1970-01-01'));
SELECT count() FROM t_date32_coercion WHERE d IN (toDate32('2024-01-01'));

DROP TABLE t_date32_coercion;
