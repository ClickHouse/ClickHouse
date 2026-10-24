-- The `INSERT ... VALUES` expression fallback in `ValuesBlockInputFormat` takes the `Field` produced by
-- `convertFieldToType` as the value to store. A numeric constant outside the calendar / clock window of a
-- `DateTime64` / `Time64` must therefore follow `date_time_overflow_behavior` there exactly like `CAST` does,
-- instead of degrading into a generic "Cannot insert NULL value" error. The fallback is reached directly when
-- template deduction is off, and also when the template - which casts - throws under `throw`, so both are covered.
--
-- `clickhouse-client` parses inline `VALUES` data itself unless `send_table_structure_on_insert_with_inline_data = 0`
-- (randomized by the test harness) hands the raw data to the server, so the expected exception may be raised on
-- either side: the `INSERT` hints use the side-agnostic `error` form.

DROP TABLE IF EXISTS t_values_overflow;
CREATE TABLE t_values_overflow (tag String, dt DateTime64(3, 'UTC'), t Time64(3)) ENGINE = Memory;

SET input_format_values_deduce_templates_of_expressions = 0;

SELECT 'throw';
SET date_time_overflow_behavior = 'throw';
INSERT INTO t_values_overflow VALUES ('int above', toUInt128('300000000000'), toInt64(0)); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_values_overflow VALUES ('int below', toInt64(-300000000000), toInt64(0)); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_values_overflow VALUES ('ticks overflow', toUInt256('100000000000000000000000000'), toInt64(0)); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_values_overflow VALUES ('float', toFloat64(1e30), toInt64(0)); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_values_overflow VALUES ('decimal', toDecimal64(300000000000, 2), toInt64(0)); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_values_overflow VALUES ('time above', toInt64(0), toInt64(3600000)); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_values_overflow VALUES ('time below', toInt64(0), toFloat64(-3.6e6)); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT count() FROM t_values_overflow;

SELECT 'saturate';
SET date_time_overflow_behavior = 'saturate';
INSERT INTO t_values_overflow VALUES ('int above', toUInt128('300000000000'), toInt64(3600000));
INSERT INTO t_values_overflow VALUES ('int below', toInt64(-300000000000), toInt64(-3600000));
INSERT INTO t_values_overflow VALUES ('ticks overflow', toUInt256('100000000000000000000000000'), toInt256('-100000000000000000000000000'));
INSERT INTO t_values_overflow VALUES ('float', toFloat64(1e30), toFloat64(-3.6e6));
INSERT INTO t_values_overflow VALUES ('decimal', toDecimal64(-300000000000, 2), toDecimal128(3600000, 20));
SELECT tag, dt, t FROM t_values_overflow ORDER BY tag;
-- The same constants through `CAST` and through the template path (which casts) materialize the same values.
SELECT tag, dt, t FROM t_values_overflow WHERE (dt, t) NOT IN (
    (CAST(toUInt128('300000000000') AS DateTime64(3, 'UTC')), CAST(toInt64(3600000) AS Time64(3))),
    (CAST(toInt64(-300000000000) AS DateTime64(3, 'UTC')), CAST(toInt64(-3600000) AS Time64(3))),
    (CAST(toUInt256('100000000000000000000000000') AS DateTime64(3, 'UTC')), CAST(toInt256('-100000000000000000000000000') AS Time64(3))),
    (CAST(toFloat64(1e30) AS DateTime64(3, 'UTC')), CAST(toFloat64(-3.6e6) AS Time64(3))),
    (CAST(toDecimal64(-300000000000, 2) AS DateTime64(3, 'UTC')), CAST(toDecimal128(3600000, 20) AS Time64(3))));
SET input_format_values_deduce_templates_of_expressions = 1;
INSERT INTO t_values_overflow VALUES ('template int above', toUInt128('300000000000'), toInt64(3600000));
SELECT count() FROM t_values_overflow WHERE tag = 'template int above' AND (dt, t) = (SELECT dt, t FROM t_values_overflow WHERE tag = 'int above');
SET input_format_values_deduce_templates_of_expressions = 0;
-- Exact set membership is untouched: the unrepresentable constant matches no row, not the saturated ones.
SELECT count() FROM t_values_overflow WHERE dt IN (toUInt128('300000000000'));

SELECT 'ignore';
SET date_time_overflow_behavior = 'ignore';
TRUNCATE TABLE t_values_overflow;
INSERT INTO t_values_overflow VALUES ('int above', toUInt128('300000000000'), toInt64(3600000));
INSERT INTO t_values_overflow VALUES ('int below', toInt64(-300000000000), toInt64(-3600000));
INSERT INTO t_values_overflow VALUES ('float', toFloat64(-1e30), toFloat64(3.6e6));
SELECT tag, dt, t FROM t_values_overflow ORDER BY tag;

-- The template path throws under `throw` as well, so the fallback must not mask the error as a NULL insertion.
SET input_format_values_deduce_templates_of_expressions = 1;
SET date_time_overflow_behavior = 'throw';
INSERT INTO t_values_overflow VALUES ('template int above', toUInt128('300000000000'), toInt64(0)); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- The `values` table function materializes through the same `Field` path.
SELECT 'values table function';
SET date_time_overflow_behavior = 'saturate';
SELECT * FROM values('dt DateTime64(3, \'UTC\'), t Time64(3)', (toUInt128('300000000000'), toInt64(-3600000)));
SET date_time_overflow_behavior = 'throw';
SELECT * FROM values('dt DateTime64(3, \'UTC\')', toUInt128('300000000000')); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

DROP TABLE t_values_overflow;
