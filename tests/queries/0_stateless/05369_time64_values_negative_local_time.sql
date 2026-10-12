-- A `DateTime` / `DateTime64` constant materialized into `Time64` by `values` and the `VALUES` expression fallback
-- gets the same local time of day as `CAST`, also behind UTC and before the epoch.

DROP TABLE IF EXISTS t_time64_values_negative_local;
SET enable_time_time64_type = 1;

SELECT CAST(toDateTime('2020-01-01 22:00:00', 'America/New_York') AS Time64(3)),
       CAST(toDateTime64('2020-01-01 22:00:00.250', 3, 'America/New_York') AS Time64(3)),
       CAST(toDateTime64('1969-12-31 23:59:59.5', 1, 'UTC') AS Time64(3));

SELECT t FROM values('t Time64(3)',
    toDateTime('2020-01-01 22:00:00', 'America/New_York'),
    toDateTime64('2020-01-01 22:00:00.250', 3, 'America/New_York'),
    toDateTime64('1969-12-31 23:59:59.5', 1, 'UTC'))
ORDER BY t;

DROP TABLE IF EXISTS t_time64_values_negative_local;
CREATE TABLE t_time64_values_negative_local (k UInt8, t Time64(3)) ENGINE = Memory;
SET input_format_values_interpret_expressions = 1, input_format_values_deduce_templates_of_expressions = 0;
INSERT INTO t_time64_values_negative_local VALUES (1, toDateTime('2020-01-01 22:00:00', 'America/New_York')), (2, toDateTime64('2020-01-01 22:00:00.250', 3, 'America/New_York')), (3, toDateTime64('1969-12-31 23:59:59.5', 1, 'UTC'));
SELECT k, t FROM t_time64_values_negative_local ORDER BY k;
DROP TABLE t_time64_values_negative_local;
