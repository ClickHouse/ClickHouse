SET use_legacy_to_time=0;
SET enable_time_time64_type=1;
SET enable_analyzer=1;

-- Time + Time -> Time
SELECT toTypeName([toTime('00:00:00'), toTime('00:00:01')]);

-- Time + Time64(0) -> Time64(0)
SELECT toTypeName([toTime('00:00:00'), toTime64('00:00:01', 0)]);

-- Time + Time64(3) -> Time64(3)
SELECT toTypeName([toTime('00:00:00'), toTime64('00:00:01', 3)]);

-- Time64(1) + Time64(3) -> Time64(3)
SELECT toTypeName([toTime64('00:00:00.1', 1), toTime64('00:00:00.123', 3)]);

-- Time64(6) + Time64(3) -> Time64(6)
SELECT toTypeName([toTime64('00:00:00.010000', 6), toTime64('00:00:00.123', 3)]);

-- Nullable with Time -> Nullable(Time)
SELECT toTypeName([NULL, toTime('00:00:00')]);

-- Nullable with Time64(6) -> Nullable(Time64(6))
SELECT toTypeName([NULL, toTime64('00:00:00', 6)]);

-- Mixing Nullable(Time) and Time64(6) -> Nullable(Time64(6))
SELECT toTypeName([toTime('00:00:00'), NULL, toTime64('00:00:00', 6)]);

-- IF(Time, Time64(6)) -> Time64(6)
SELECT toTypeName(if(1, toTime('00:00:00'), toTime64('00:00:00', 6)));

-- IF(Time64(3), Time64(6)) -> Time64(6)
SELECT toTypeName(if(0, toTime64('00:00:00', 3), toTime64('00:00:00', 6)));

-- multiIf with mixture -> Time64(6)
SELECT toTypeName(multiIf(1, toTime64('00:00:00', 3), 0, toTime64('00:00:00', 6), toTime('00:00:00')));

-- UNION ALL unifies to max scale (Time + Time64(6) -> Time64(6)); use LIMIT 1 to avoid duplicate lines
SELECT toTypeName(x) FROM
(
    SELECT toTime('00:00:00') AS x
    UNION ALL
    SELECT toTime64('00:00:00', 6)
)
LIMIT 1;

-- Time with Int -> NO_COMMON_TYPE
SELECT toTypeName([toTime('00:00:00'), 1]); -- { serverError NO_COMMON_TYPE }

-- Time64 with Date -> NO_COMMON_TYPE
SELECT toTypeName([toTime64('00:00:00', 3), toDate('2020-01-01')]); -- { serverError NO_COMMON_TYPE }

-- Time64 with DateTime -> NO_COMMON_TYPE
SELECT toTypeName([toTime64('00:00:00', 3), toDateTime('2020-01-01 00:00:00')]); -- { serverError NO_COMMON_TYPE }
