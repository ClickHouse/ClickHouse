SET enable_time_time64_type = 1;
SET use_legacy_to_time = 0;
SET session_timezone = 'UTC';

SELECT '-- Original Time aggregate functions test';

DROP TABLE IF EXISTS dt;
CREATE TABLE dt
(
    `time` Time,
    `event_id` UInt8
)
ENGINE = TinyLog;

INSERT INTO dt VALUES ('100:00:00', 1), (12453, 3);

SELECT max(time) FROM dt;
SELECT min(time) FROM dt;
SELECT avg(time) FROM dt;

DROP TABLE dt;

SELECT '-- Date type basic test';
SELECT avg(d) FROM (SELECT toDate('2020-01-01') AS d UNION ALL SELECT toDate('2020-01-03') AS d);
SELECT toTypeName(avg(d)) FROM (SELECT toDate('2020-01-01') AS d UNION ALL SELECT toDate('2020-01-03') AS d);

SELECT '-- Date32 type basic test';
SELECT avg(d) FROM (SELECT toDate32('2020-01-01') AS d UNION ALL SELECT toDate32('2020-01-03') AS d);
SELECT toTypeName(avg(d)) FROM (SELECT toDate32('2020-01-01') AS d UNION ALL SELECT toDate32('2020-01-03') AS d);

SELECT '-- DateTime type basic test';
SELECT avg(dt) FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') AS dt UNION ALL SELECT toDateTime('2020-01-01 00:00:02', 'UTC') AS dt);
SELECT toTypeName(avg(dt)) FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') AS dt);

SELECT '-- DateTime64 type basic test (scale 3)';
SELECT avg(dt64) FROM (SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') AS dt64 UNION ALL SELECT toDateTime64('2020-01-01 00:00:00.002', 3, 'UTC') AS dt64);
SELECT toTypeName(avg(dt64)) FROM (SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') AS dt64);

SELECT '-- Time type basic test';
SELECT avg(t) FROM (SELECT toTime('01:00:00') AS t UNION ALL SELECT toTime('03:00:00') AS t);
SELECT toTypeName(avg(t)) FROM (SELECT toTime('01:00:00') AS t);

SELECT '-- Time64 type basic test (scale 3)';
SELECT avg(t64) FROM (SELECT toTime64('01:00:00.000', 3) AS t64 UNION ALL SELECT toTime64('03:00:00.000', 3) AS t64);
SELECT toTypeName(avg(t64)) FROM (SELECT toTime64('01:00:00.000', 3) AS t64);

SELECT '-- Empty table tests (NaN -> 0/epoch)';
DROP TABLE IF EXISTS empty_date_test;
CREATE TABLE empty_date_test (d Date, d32 Date32, dt DateTime('UTC'), dt64 DateTime64(3, 'UTC'), t Time, t64 Time64(3)) ENGINE = Memory;

SELECT avg(d), avg(d32), avg(dt), avg(dt64), avg(t), avg(t64) FROM empty_date_test FORMAT Vertical;

DROP TABLE empty_date_test;

SELECT '-- Single row tests';
SELECT avg(d) FROM (SELECT toDate('2020-06-15') AS d);
SELECT avg(d32) FROM (SELECT toDate32('2020-06-15') AS d32);
SELECT avg(dt) FROM (SELECT toDateTime('2020-06-15 12:30:45', 'UTC') AS dt);
SELECT avg(dt64) FROM (SELECT toDateTime64('2020-06-15 12:30:45.500', 3, 'UTC') AS dt64);
SELECT avg(t) FROM (SELECT toTime('12:30:45') AS t);
SELECT avg(t64) FROM (SELECT toTime64('12:30:45.500', 3) AS t64);

SELECT '-- Multiple identical values tests';
SELECT avg(d) FROM (SELECT toDate('2020-06-15') AS d FROM numbers(5));
SELECT avg(dt) FROM (SELECT toDateTime('2020-06-15 12:30:45', 'UTC') AS dt FROM numbers(5));
SELECT avg(dt64) FROM (SELECT toDateTime64('2020-06-15 12:30:45.123', 3, 'UTC') AS dt64 FROM numbers(5));
SELECT avg(t) FROM (SELECT toTime('12:30:45') AS t FROM numbers(5));
SELECT avg(t64) FROM (SELECT toTime64('12:30:45.123', 3) AS t64 FROM numbers(5));

SELECT '-- Rounding behavior tests';

SELECT '-- Date rounding: 2020-01-01 (day 18262) and 2020-01-02 (day 18263) -> avg = 18262.5 -> rounds to 18262 (banker)';
SELECT avg(d) FROM (SELECT toDate('2020-01-01') AS d UNION ALL SELECT toDate('2020-01-02') AS d);

SELECT '-- Date exact avg: 2020-01-01, 2020-01-02, 2020-01-03 -> avg = day 18263 exactly';
SELECT avg(d) FROM (SELECT toDate('2020-01-01') AS d UNION ALL SELECT toDate('2020-01-02') AS d UNION ALL SELECT toDate('2020-01-03') AS d);

SELECT '-- DateTime rounding: second 0 and second 1 -> avg = 0.5 -> rounds to 0';
SELECT avg(dt) FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') AS dt UNION ALL SELECT toDateTime('2020-01-01 00:00:01', 'UTC') AS dt);

SELECT '-- DateTime rounding: second 1 and second 2 -> avg = 1.5 -> rounds to 2 (banker)';
SELECT avg(dt) FROM (SELECT toDateTime('2020-01-01 00:00:01', 'UTC') AS dt UNION ALL SELECT toDateTime('2020-01-01 00:00:02', 'UTC') AS dt);

SELECT '-- Time rounding tests';
SELECT avg(t) FROM (SELECT toTime('00:00:00') AS t UNION ALL SELECT toTime('00:00:01') AS t); -- 0.5 -> 0
SELECT avg(t) FROM (SELECT toTime('00:00:01') AS t UNION ALL SELECT toTime('00:00:02') AS t); -- 1.5 -> 2

SELECT '-- DateTime64 rounding with subsecond precision';
SELECT avg(dt64) FROM (SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') AS dt64 UNION ALL SELECT toDateTime64('2020-01-01 00:00:00.001', 3, 'UTC') AS dt64);

SELECT '-- Time64 rounding with subsecond precision';
SELECT avg(t64) FROM (SELECT toTime64('00:00:00.000', 3) AS t64 UNION ALL SELECT toTime64('00:00:00.001', 3) AS t64);

SELECT '-- DateTime64/Time64 different scales';

SELECT avg(dt64) FROM (SELECT toDateTime64('2020-01-01 00:00:00', 0, 'UTC') AS dt64 UNION ALL SELECT toDateTime64('2020-01-01 00:00:02', 0, 'UTC') AS dt64);
SELECT toTypeName(avg(dt64)) FROM (SELECT toDateTime64('2020-01-01 00:00:00', 0, 'UTC') AS dt64);

SELECT avg(dt64) FROM (SELECT toDateTime64('2020-01-01 00:00:00.000000', 6, 'UTC') AS dt64 UNION ALL SELECT toDateTime64('2020-01-01 00:00:00.000002', 6, 'UTC') AS dt64);
SELECT toTypeName(avg(dt64)) FROM (SELECT toDateTime64('2020-01-01 00:00:00.000000', 6, 'UTC') AS dt64);

SELECT avg(dt64) FROM (SELECT toDateTime64('2020-01-01 00:00:00.000000000', 9, 'UTC') AS dt64 UNION ALL SELECT toDateTime64('2020-01-01 00:00:00.000000002', 9, 'UTC') AS dt64);
SELECT toTypeName(avg(dt64)) FROM (SELECT toDateTime64('2020-01-01 00:00:00.000000000', 9, 'UTC') AS dt64);

SELECT avg(t64) FROM (SELECT toTime64('01:00:00', 0) AS t64 UNION ALL SELECT toTime64('03:00:00', 0) AS t64);
SELECT toTypeName(avg(t64)) FROM (SELECT toTime64('01:00:00', 0) AS t64);

SELECT avg(t64) FROM (SELECT toTime64('01:00:00.000000', 6) AS t64 UNION ALL SELECT toTime64('03:00:00.000000', 6) AS t64);
SELECT toTypeName(avg(t64)) FROM (SELECT toTime64('01:00:00.000000', 6) AS t64);

SELECT '-- Boundary value tests';

SELECT '-- Date boundary';
SELECT avg(d) FROM (SELECT toDate('1970-01-01') AS d UNION ALL SELECT toDate('2100-12-31') AS d);

SELECT '-- Date32 with pre-1970 dates';
SELECT avg(d32) FROM (SELECT toDate32('1900-01-01') AS d32 UNION ALL SELECT toDate32('2100-01-01') AS d32);

SELECT '-- DateTime boundary';
SELECT avg(dt) FROM (SELECT toDateTime('1970-01-01 00:00:00', 'UTC') AS dt UNION ALL SELECT toDateTime('2100-01-01 00:00:00', 'UTC') AS dt);

SELECT '-- Time extended range';
SELECT avg(t) FROM (SELECT toTime('00:00:00') AS t UNION ALL SELECT toTime('100:00:00') AS t);

SELECT '-- NULL handling tests';

DROP TABLE IF EXISTS nullable_date_test;
CREATE TABLE nullable_date_test (
    d Nullable(Date),
    d32 Nullable(Date32),
    dt Nullable(DateTime('UTC')),
    dt64 Nullable(DateTime64(3, 'UTC')),
    t Nullable(Time),
    t64 Nullable(Time64(3))
) ENGINE = Memory;

INSERT INTO nullable_date_test VALUES 
    ('2020-01-01', '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.000', '01:00:00', '01:00:00.000'),
    (NULL, NULL, NULL, NULL, NULL, NULL),
    ('2020-01-03', '2020-01-03', '2020-01-01 00:00:02', '2020-01-01 00:00:00.002', '03:00:00', '03:00:00.000');

SELECT avg(d), avg(d32), avg(dt), avg(dt64), avg(t), avg(t64) FROM nullable_date_test FORMAT Vertical;

SELECT avgOrNull(d), avgOrNull(d32), avgOrNull(dt), avgOrNull(dt64), avgOrNull(t), avgOrNull(t64) FROM nullable_date_test FORMAT Vertical;

SELECT avg(d) FROM nullable_date_test WHERE d IS NULL;

DROP TABLE nullable_date_test;

SELECT '-- avgIf conditional aggregation tests';

DROP TABLE IF EXISTS avgif_date_test;
CREATE TABLE avgif_date_test (
    d Date,
    dt DateTime('UTC'),
    t Time,
    flag UInt8
) ENGINE = Memory;

INSERT INTO avgif_date_test VALUES 
    ('2020-01-01', '2020-01-01 00:00:00', '01:00:00', 1),
    ('2020-01-02', '2020-01-01 00:00:01', '02:00:00', 0),
    ('2020-01-03', '2020-01-01 00:00:02', '03:00:00', 1);

SELECT avgIf(d, flag = 1), avgIf(dt, flag = 1), avgIf(t, flag = 1) FROM avgif_date_test;

SELECT avgIf(d, flag = 99) FROM avgif_date_test;

DROP TABLE avgif_date_test;

SELECT '-- GROUP BY tests';

DROP TABLE IF EXISTS groupby_date_test;
CREATE TABLE groupby_date_test (
    grp String,
    d Date,
    dt DateTime('UTC'),
    t Time
) ENGINE = Memory;

INSERT INTO groupby_date_test VALUES 
    ('A', '2020-01-01', '2020-01-01 00:00:00', '01:00:00'),
    ('A', '2020-01-03', '2020-01-01 00:00:02', '03:00:00'),
    ('B', '2020-06-15', '2020-06-15 12:00:00', '12:00:00'),
    ('B', '2020-06-17', '2020-06-15 12:00:02', '14:00:00');

SELECT grp, avg(d), avg(dt), avg(t) FROM groupby_date_test GROUP BY grp ORDER BY grp;

DROP TABLE groupby_date_test;

SELECT '-- Subquery tests';

SELECT avg(d) FROM (
    SELECT addDays(toDate('2020-01-01'), number) AS d FROM numbers(10)
);

SELECT avg(dt) FROM (
    SELECT addSeconds(toDateTime('2020-01-01 00:00:00', 'UTC'), number) AS dt FROM numbers(10)
);

SELECT avg(t) FROM (
    SELECT CAST(number AS Time) AS t FROM numbers(10)
);

SELECT '-- Large dataset tests';

SELECT avg(d) FROM (
    SELECT addDays(toDate('2020-01-01'), number) AS d FROM numbers(1000)
);

SELECT avg(dt) FROM (
    SELECT addSeconds(toDateTime('2020-01-01 00:00:00', 'UTC'), number) AS dt FROM numbers(1000)
);

SELECT '-- Mixed aggregate functions';

SELECT 
    min(d), max(d), avg(d),
    min(dt), max(dt), avg(dt),
    min(t), max(t), avg(t)
FROM (
    SELECT 
        addDays(toDate('2020-01-01'), number) AS d,
        addSeconds(toDateTime('2020-01-01 00:00:00', 'UTC'), number) AS dt,
        CAST(number AS Time) AS t
    FROM numbers(5)
) FORMAT Vertical;

SELECT '-- Window function tests';

SELECT 
    d,
    avg(d) OVER (ORDER BY d ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg
FROM (
    SELECT addDays(toDate('2020-01-01'), number) AS d FROM numbers(5)
)
ORDER BY d;

SELECT '-- State merge tests';

SELECT avgMerge(state) FROM (
    SELECT avgState(d) AS state FROM (SELECT toDate('2020-01-01') AS d UNION ALL SELECT toDate('2020-01-02') AS d)
    UNION ALL
    SELECT avgState(d) AS state FROM (SELECT toDate('2020-01-03') AS d UNION ALL SELECT toDate('2020-01-04') AS d)
);

SELECT avgMerge(state) FROM (
    SELECT avgState(dt) AS state FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') AS dt UNION ALL SELECT toDateTime('2020-01-01 00:00:02', 'UTC') AS dt)
    UNION ALL
    SELECT avgState(dt) AS state FROM (SELECT toDateTime('2020-01-01 00:00:04', 'UTC') AS dt UNION ALL SELECT toDateTime('2020-01-01 00:00:06', 'UTC') AS dt)
);

SELECT '-- Result type verification';

SELECT 'Date result type:', toTypeName(avg(d)) FROM (SELECT toDate('2020-01-01') AS d);
SELECT 'Date32 result type:', toTypeName(avg(d32)) FROM (SELECT toDate32('2020-01-01') AS d32);
SELECT 'DateTime result type:', toTypeName(avg(dt)) FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') AS dt);
SELECT 'DateTime64(3) result type:', toTypeName(avg(dt64)) FROM (SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') AS dt64);
SELECT 'Time result type:', toTypeName(avg(t)) FROM (SELECT toTime('01:00:00') AS t);
SELECT 'Time64(3) result type:', toTypeName(avg(t64)) FROM (SELECT toTime64('01:00:00.000', 3) AS t64);

SELECT '-- Expressions with avg results';

SELECT avg(d) + 1 FROM (SELECT toDate('2020-01-01') AS d UNION ALL SELECT toDate('2020-01-03') AS d);

SELECT avg(dt) + 1 FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') AS dt UNION ALL SELECT toDateTime('2020-01-01 00:00:02', 'UTC') AS dt);

SELECT '-- Comparison of avg results';

SELECT 
    avg(d) > toDate('2020-01-01'),
    avg(d) = toDate('2020-01-02')
FROM (SELECT toDate('2020-01-01') AS d UNION ALL SELECT toDate('2020-01-03') AS d);

SELECT 
    avg(dt) > toDateTime('2020-01-01 00:00:00', 'UTC'),
    avg(dt) = toDateTime('2020-01-01 00:00:01', 'UTC')
FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') AS dt UNION ALL SELECT toDateTime('2020-01-01 00:00:02', 'UTC') AS dt);

SELECT '-- Timezone tests';

SELECT avg(dt) FROM (
    SELECT toDateTime('2020-01-01 00:00:00', 'Europe/London') AS dt 
    UNION ALL 
    SELECT toDateTime('2020-01-01 00:00:02', 'Europe/London') AS dt
);

SELECT avg(dt64) FROM (
    SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'America/New_York') AS dt64 
    UNION ALL 
    SELECT toDateTime64('2020-01-01 00:00:00.002', 3, 'America/New_York') AS dt64
);

SELECT '-- Precision edge cases';

SELECT avg(dt64) FROM (
    SELECT toDateTime64('2020-01-01 12:00:00.001', 3, 'UTC') AS dt64 
    UNION ALL 
    SELECT toDateTime64('2020-01-01 12:00:00.002', 3, 'UTC') AS dt64
    UNION ALL 
    SELECT toDateTime64('2020-01-01 12:00:00.003', 3, 'UTC') AS dt64
);

SELECT avg(t64) FROM (
    SELECT toTime64('12:00:00.001', 3) AS t64 
    UNION ALL 
    SELECT toTime64('12:00:00.002', 3) AS t64
    UNION ALL 
    SELECT toTime64('12:00:00.003', 3) AS t64
);

SELECT '-- JIT disabled verification (should work correctly regardless)';
SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

SELECT avg(d) FROM (SELECT addDays(toDate('2020-01-01'), number) AS d FROM numbers(100));
SELECT avg(dt) FROM (SELECT addSeconds(toDateTime('2020-01-01 00:00:00', 'UTC'), number) AS dt FROM numbers(100));
SELECT avg(dt64) FROM (SELECT addSeconds(toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC'), number) AS dt64 FROM numbers(100));
-- Use CAST for Time types to avoid timezone sensitivity
DROP TABLE IF EXISTS jit_time_test;
CREATE TABLE jit_time_test (t Time, t64 Time64(3)) ENGINE = Memory;
INSERT INTO jit_time_test SELECT CAST(number AS Time), CAST(number AS Time64(3)) FROM numbers(100);
SELECT avg(t) FROM jit_time_test;
SELECT avg(t64) FROM jit_time_test;
DROP TABLE jit_time_test;

SET compile_aggregate_expressions = 0;

SELECT '-- Negative Time values test';

DROP TABLE IF EXISTS negative_time_test;
CREATE TABLE negative_time_test (t Time) ENGINE = Memory;
-- Insert -3600 (=-1h), 3600 (=1h), 10800 (=3h) -> avg = 3600 = 1h
INSERT INTO negative_time_test SELECT toTime(-3600 + number * 7200) FROM numbers(3);

SELECT avg(t) FROM negative_time_test;

DROP TABLE negative_time_test;

SELECT '-- Materialized view state test';

DROP TABLE IF EXISTS mv_source;
DROP TABLE IF EXISTS mv_target;

CREATE TABLE mv_source (grp UInt8, d Date, dt DateTime('UTC'), t Time) ENGINE = MergeTree ORDER BY grp;
CREATE TABLE mv_target (
    grp UInt8,
    d_avg AggregateFunction(avg, Date),
    dt_avg AggregateFunction(avg, DateTime('UTC')),
    t_avg AggregateFunction(avg, Time)
) ENGINE = AggregatingMergeTree ORDER BY grp;

CREATE MATERIALIZED VIEW mv_view TO mv_target AS
SELECT 1 AS grp, avgState(d) AS d_avg, avgState(dt) AS dt_avg, avgState(t) AS t_avg
FROM mv_source
GROUP BY grp;

INSERT INTO mv_source VALUES (1, '2020-01-01', '2020-01-01 00:00:00', '01:00:00');
INSERT INTO mv_source VALUES (1, '2020-01-03', '2020-01-01 00:00:02', '03:00:00');

SELECT avgMerge(d_avg), avgMerge(dt_avg), avgMerge(t_avg) FROM mv_target;

DROP VIEW mv_view;
DROP TABLE mv_target;
DROP TABLE mv_source;
