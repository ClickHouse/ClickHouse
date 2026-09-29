DROP TABLE IF EXISTS t_dt64_in_set;

-- A `UInt64` constant that no `DateTime64` value can equal must not be turned into a tick count that
-- wrapped around: `IN` matched a pre-epoch row and `NOT IN` lost it, while `=` matched nothing.

CREATE TABLE t_dt64_in_set (dt DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_dt64_in_set VALUES ('1969-12-31 23:59:59'), ('1970-01-01 00:00:01'), ('2000-01-01 00:00:00');

SELECT count() FROM t_dt64_in_set WHERE dt = toUInt64(18446744073709551615);
SELECT count() FROM t_dt64_in_set WHERE dt IN (toUInt64(18446744073709551615));
SELECT count() FROM t_dt64_in_set WHERE dt NOT IN (toUInt64(18446744073709551615));

-- An in-range constant is unaffected.
SELECT count() FROM t_dt64_in_set WHERE dt IN (toUInt64(1));
SELECT count() FROM t_dt64_in_set WHERE dt NOT IN (toUInt64(1));
SELECT count() FROM t_dt64_in_set WHERE dt IN (toInt64(-1));

DROP TABLE t_dt64_in_set;

-- A finer scale rejects more constants: the number of seconds must still fit the tick count.

CREATE TABLE t_dt64_in_set (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_dt64_in_set VALUES ('1969-12-31 23:59:59.999'), ('1970-01-01 00:00:01.000'), ('2000-01-01 00:00:00.000');

SELECT count() FROM t_dt64_in_set WHERE dt IN (toUInt64(18446744073709551615));
SELECT count() FROM t_dt64_in_set WHERE dt NOT IN (toUInt64(18446744073709551615));
SELECT count() FROM t_dt64_in_set WHERE dt IN (toUInt64(9223372036854776));
SELECT count() FROM t_dt64_in_set WHERE dt IN (toUInt64(1));

DROP TABLE t_dt64_in_set;

SELECT toTime64(-1, 0) IN (toUInt64(18446744073709551615));
SELECT toTime64(-1, 0) IN (toInt64(-1));

-- The same conversion materializes a value in `values`, where an unrepresentable constant is reported
-- rather than stored as a wrapped tick count.

SELECT x FROM values('x DateTime64(3, \'UTC\')', toUInt64(18446744073709551615)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT x FROM values('x DateTime64(3, \'UTC\')', toUInt64(9223372036854776)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT x FROM values('x DateTime64(3, \'UTC\')', toUInt64(1));
SELECT x FROM values('x Time64(0)', toUInt64(18446744073709551615)); -- { serverError ARGUMENT_OUT_OF_BOUND }
