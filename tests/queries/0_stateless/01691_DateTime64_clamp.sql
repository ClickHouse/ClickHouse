-- { echo }
SELECT toDateTime(-2, 2);
SELECT toDateTime64(-2, 2);
SELECT CAST(-1 AS DateTime64);
SELECT CAST('2020-01-01 00:00:00.3' AS DateTime64);
