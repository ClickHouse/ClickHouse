-- { echo }

DROP TABLE IF EXISTS t;
CREATE TABLE t (s AggregateFunction(cramersV, UInt8, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT cramersVState(a, b) OVER () FROM (SELECT toNullable(toUInt8(number % 10)) AS a, toNullable(toUInt8(number % 6)) AS b FROM numbers(100)) LIMIT 1; -- {serverError CANNOT_CONVERT_TYPE}
