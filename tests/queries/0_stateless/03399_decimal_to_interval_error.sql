-- https://github.com/ClickHouse/ClickHouse/issues/75451
-- Decimal types should produce a clear error when converting to Interval, even on empty tables.

SELECT toIntervalMillisecond(CAST(1 AS Decimal(18, 3))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toIntervalSecond(CAST(1 AS Decimal(18, 3))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toIntervalMillisecond(CAST(1 AS Nullable(Decimal(18, 3)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS t;
CREATE TABLE t (d Nullable(Decimal(18, 3))) ENGINE = MergeTree ORDER BY tuple();
SELECT toIntervalMillisecond(d) FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t;
