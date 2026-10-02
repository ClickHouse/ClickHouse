-- A `DateTime` stores a `UInt32`, so a `UInt64` constant that does not fit it cannot equal any value
-- of the column. The set builder took such a constant unchanged, and the column insertion truncated
-- it modulo 2^32: `dt IN (toUInt64(4294967296))` matched the epoch row, and the `OR` chain that the
-- disjunction rewrite turns into `IN` returned every row it wrapped onto.

DROP TABLE IF EXISTS t_datetime_set;
CREATE TABLE t_datetime_set (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_datetime_set VALUES (0), (1), (2);

SELECT 'a constant that does not fit the column';
SELECT count() FROM t_datetime_set WHERE dt = toUInt64(4294967296);
SELECT count() FROM t_datetime_set WHERE dt IN (toUInt64(4294967296));
SELECT count() FROM t_datetime_set WHERE dt NOT IN (toUInt64(4294967296));

SELECT 'an OR chain of equalities, which the rewrite turns into IN';
SELECT count() FROM t_datetime_set
WHERE dt = toUInt64(4294967296) OR dt = toUInt64(4294967297) OR dt = toUInt64(4294967298);
SELECT count() FROM t_datetime_set
WHERE dt = toUInt64(4294967296) OR dt = toUInt64(4294967297) OR dt = toUInt64(4294967298)
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'a mixed set keeps the members that do fit';
SELECT count() FROM t_datetime_set WHERE dt IN (toUInt64(1), toUInt64(4294967296));
SELECT count() FROM t_datetime_set WHERE dt NOT IN (toUInt64(1), toUInt64(4294967296));

SELECT 'constants that do fit are unchanged';
SELECT count() FROM t_datetime_set WHERE dt IN (toUInt64(0), toUInt64(2));
SELECT count() FROM t_datetime_set WHERE dt IN (toUInt64(4294967295));
SELECT count() FROM t_datetime_set WHERE dt IN (toDateTime(1, 'UTC'));

SELECT 'the values table function takes the same path';
SELECT x FROM values('x DateTime(''UTC'')', toUInt64(1));
SELECT CAST(toUInt64(1), 'DateTime(''UTC'')');
-- A constant the column cannot hold is rejected there, as it is for `Date`, `Date32` and `UInt8`.
SELECT x FROM values('x DateTime(''UTC'')', toUInt64(4294967296)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT x FROM values('x Date', toUInt64(65536)); -- { serverError ARGUMENT_OUT_OF_BOUND }

DROP TABLE t_datetime_set;
