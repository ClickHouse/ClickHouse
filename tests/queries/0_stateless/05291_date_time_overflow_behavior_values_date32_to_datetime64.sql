-- A `Date32` constant whose midnight does not fit the `Int64` ticks of a high-scale `DateTime64` must follow
-- `date_time_overflow_behavior` on the `Field` materialization paths (the `INSERT ... VALUES` expression fallback and
-- the `values` table function) exactly like `CAST` does, instead of degrading into a generic NULL insertion error.
--
-- `clickhouse-client` parses inline `VALUES` data itself unless `send_table_structure_on_insert_with_inline_data = 0`
-- (randomized by the test harness) hands the raw data to the server, so the expected exception may be raised on
-- either side: the `INSERT` hints use the side-agnostic `error` form.

DROP TABLE IF EXISTS t_values_date32_overflow;
CREATE TABLE t_values_date32_overflow (tag String, dt DateTime64(9, 'UTC')) ENGINE = Memory;

SET input_format_values_deduce_templates_of_expressions = 0;

SELECT 'throw';
SET date_time_overflow_behavior = 'throw';
INSERT INTO t_values_date32_overflow VALUES ('above', toDate32('2299-12-31')); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_values_date32_overflow VALUES ('below', toDate32('1600-01-01')); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT * FROM values('dt DateTime64(9, \'UTC\')', toDate32('2299-12-31')); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT count() FROM t_values_date32_overflow;

SELECT 'saturate';
SET date_time_overflow_behavior = 'saturate';
INSERT INTO t_values_date32_overflow VALUES ('above', toDate32('2299-12-31'));
INSERT INTO t_values_date32_overflow VALUES ('below', toDate32('1600-01-01'));
INSERT INTO t_values_date32_overflow VALUES ('in range', toDate32('2000-01-01'));
SELECT tag, dt FROM t_values_date32_overflow ORDER BY tag;
-- The same constants through `CAST` materialize the same values.
SELECT tag, dt FROM t_values_date32_overflow WHERE dt NOT IN (
    CAST(toDate32('2299-12-31') AS DateTime64(9, 'UTC')),
    CAST(toDate32('1600-01-01') AS DateTime64(9, 'UTC')),
    CAST(toDate32('2000-01-01') AS DateTime64(9, 'UTC')));
SELECT * FROM values('dt DateTime64(9, \'UTC\')', toDate32('2299-12-31'), toDate32('1600-01-01'));
-- Exact set membership is untouched: the unrepresentable constant matches no row, not the saturated one.
SELECT count() FROM t_values_date32_overflow WHERE dt IN (toDate32('2299-12-31'));

DROP TABLE t_values_date32_overflow;

-- A non-NULL `Nullable(Date32)` constant is a plain `Date32` value: it is converted from its day number (midnight of
-- that day), not reinterpreted as a count of seconds, and its overflow follows `date_time_overflow_behavior` too.
SELECT 'nullable';
SELECT * FROM values('dt DateTime64(3, \'UTC\')', CAST(toDate32('1970-01-02') AS Nullable(Date32)));
SELECT * FROM values('dt DateTime64(9, \'UTC\')', CAST(toDate32('2299-12-31') AS Nullable(Date32)));
SET date_time_overflow_behavior = 'throw';
SELECT * FROM values('dt DateTime64(9, \'UTC\')', CAST(toDate32('2299-12-31') AS Nullable(Date32))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
