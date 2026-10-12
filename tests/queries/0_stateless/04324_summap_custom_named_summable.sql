-- Regression test for reverting PR #98976: `sumMap` / the `-Map` combinator
-- must accept custom-named numeric value types again. Before the revert these
-- threw `ILLEGAL_TYPE_OF_ARGUMENT: Values for -Map cannot be summed` because
-- `DataTypeNumber::isSummable` returned false for custom-named types.

-- `Bool` is a custom-named `UInt8`; the value is promoted to `UInt64` and summed as a count.
SELECT sumMap([1], [true]);
SELECT sumMap([1, 2], [true, false]);
SELECT sumMapWithOverflow([1, 2], [true, false]);

-- `SimpleAggregateFunction(sum, UInt64)` is physically `UInt64` with a custom name;
-- rolling up pre-aggregated counters with `sumMap` is a common, correct pattern.
DROP TABLE IF EXISTS t_04324;

CREATE TABLE t_04324 (k UInt32, cnt SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY k;

INSERT INTO t_04324 VALUES (1, 5), (1, 7), (2, 3);

SELECT sumMap([k], [cnt]) FROM t_04324;

DROP TABLE t_04324;
