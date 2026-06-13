-- { echo }

-- Descending series with negative step
SELECT * FROM generate_series(9, 0, -1);
SELECT * FROM generate_series(10, 0, -3);
SELECT * FROM generate_series(99, 0, -1) LIMIT 5;

-- Empty result: negative step but start < stop
SELECT count() FROM generate_series(0, 10, -1);

-- Empty result: positive step but start > stop (existing behavior)
SELECT count() FROM generate_series(10, 0, 1);

-- Step of -1 with equal start and stop
SELECT * FROM generate_series(5, 5, -1);

-- Larger negative step
SELECT * FROM generate_series(100, 0, -25);

-- Count with negative step
SELECT count() FROM generate_series(99, 0, -1);
SELECT count() FROM generate_series(1000, 0, -3);

-- Sum with negative step
SELECT sum(generate_series) FROM generate_series(10, 0, -1);

-- Large positive UInt64 step (backward compatibility)
SELECT * FROM generate_series(0, 10, toUInt64(9223372036854775808));

-- Overflow guard: cardinality exceeds UInt64
SELECT * FROM generate_series(18446744073709551615, 0, -1); -- { serverError BAD_ARGUMENTS }
SELECT * FROM generate_series(0, 18446744073709551615, 1); -- { serverError BAD_ARGUMENTS }

-- INT64_MIN as step (boundary for signed negation)
SELECT count() FROM generate_series(9223372036854775807, 0, -9223372036854775808);

-- Full UInt64 range with large step: previously silently returned empty result,
-- now uses simple stepped mode to produce correct output.
SELECT * FROM generate_series(0, CAST('18446744073709551615', 'UInt64'), toUInt64(9223372036854775808));

SELECT * FROM generate_series(CAST('18446744073709551615', 'UInt64'), 0, -9223372036854775808);

-- Simple stepped ascending: LIMIT and WHERE
SELECT * FROM generate_series(0, CAST('18446744073709551615', 'UInt64'), toUInt64(9223372036854775808)) LIMIT 1;
SELECT * FROM generate_series(0, CAST('18446744073709551615', 'UInt64'), toUInt64(9223372036854775808)) WHERE generate_series > 0;
SELECT count() FROM generate_series(0, CAST('18446744073709551615', 'UInt64'), toUInt64(9223372036854775808));

-- WHERE filter on descending series (no filter pushdown)
SELECT * FROM generate_series(20, 0, -3) WHERE generate_series > 10;

-- generateSeries alias with negative step
SELECT * FROM generateSeries(5, 0, -2);

-- start == stop with negative step, abs_step > 1
SELECT * FROM generate_series(5, 5, -3);

-- Descending series with step that exactly divides range
SELECT * FROM generate_series(10, 0, -2);

-- LIMIT with WHERE on descending series: LIMIT must not be pushed down past the filter
SELECT * FROM generate_series(10, 0, -1) WHERE generate_series < 3 LIMIT 1;

-- Zero step should error
SELECT * FROM generate_series(0, 10, 0); -- { serverError BAD_ARGUMENTS }
