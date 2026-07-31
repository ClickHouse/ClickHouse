-- Lossy DateTime64/Time64 set probes must not match after truncation or saturation.
SET enable_time_time64_type = 1;

-- Upper overflow saturates to the maximum, lower overflow to zero; exact boundary stays accepted.
SELECT materialize(CAST(toDecimal64(4294967296, 0) AS DateTime64(0))) IN (SELECT toDateTime(4294967295));
SELECT materialize(CAST(toDecimal64(-1, 0) AS DateTime64(0))) IN (SELECT toDateTime(0));
SELECT materialize(CAST(toDecimal64(4294967295, 0) AS DateTime64(0))) IN (SELECT toDateTime(4294967295));

-- Fractional and out-of-range DateTime64 values nested in Array/Tuple/Map.
SELECT [CAST('2020-01-01 00:00:00.5' AS DateTime64(1))] IN (SELECT [CAST('2020-01-01 00:00:00' AS DateTime)]);
SELECT [CAST('2020-01-01 00:00:00.0' AS DateTime64(1))] IN (SELECT [CAST('2020-01-01 00:00:00' AS DateTime)]);
SELECT [CAST(toDecimal64(4294967296, 0) AS DateTime64(0))] IN (SELECT [toDateTime(4294967295)]);
SELECT map('k', CAST('2020-01-01 00:00:00.5' AS DateTime64(1))) IN (SELECT map('k', CAST('2020-01-01 00:00:00' AS DateTime)));
SELECT (CAST('2020-01-01 00:00:00.5' AS DateTime64(1)), 1) IN (SELECT (CAST('2020-01-01 00:00:00' AS DateTime), 1));

-- A NULL element with a hidden fractional payload is not lossy; NULL elements compare equal inside arrays.
SELECT [nullIf(CAST('2020-01-01 00:00:00.5' AS DateTime64(1)), CAST('2020-01-01 00:00:00.5' AS DateTime64(1)))] IN (SELECT [CAST(NULL AS Nullable(DateTime))]);

-- Mixed tuples: the non-temporal element keeps accurate-cast semantics, the temporal one is loss-checked.
SELECT (CAST('01:02:03.5' AS Time64(1)), CAST(1 AS UInt8)) IN (SELECT (CAST('01:02:03' AS Time), CAST(1 AS UInt16)));
SELECT (CAST('01:02:03.0' AS Time64(1)), CAST(1 AS UInt8)) IN (SELECT (CAST('01:02:03' AS Time), CAST(1 AS UInt16)));
SELECT (CAST('2020-01-01 00:00:00.5' AS DateTime64(1)), CAST(1 AS UInt8)) IN (SELECT (CAST('2020-01-01 00:00:00' AS DateTime), CAST(1 AS UInt16)));

-- Named tuples are matched by name, like the tuple cast does.
SELECT CAST((CAST('01:02:03.5' AS Time64(1)), 42) AS Tuple(t Time64(1), u UInt8)) IN (SELECT CAST((CAST(42 AS UInt16), CAST('01:02:03' AS Time)) AS Tuple(u UInt16, t Time)));
SELECT CAST((CAST('01:02:03.0' AS Time64(1)), 42) AS Tuple(t Time64(1), u UInt8)) IN (SELECT CAST((CAST(42 AS UInt16), CAST('01:02:03' AS Time)) AS Tuple(u UInt16, t Time)));
