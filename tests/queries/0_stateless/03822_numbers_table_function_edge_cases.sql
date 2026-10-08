-- From PR: https://github.com/ClickHouse/ClickHouse/pull/94288
-- Fix for GitHub issue https://github.com/ClickHouse/ClickHouse/issues/93913:
-- Numbers function can result in infinite loop with large offset and WHERE clause.

-- { echo }

-- Basic query without WHERE clause (used to work correctly)
SELECT * FROM numbers(18446744073709551611, 2);

-- Query with WHERE number (previously caused infinite loop)
SELECT * FROM numbers(18446744073709551611, 2) WHERE number;

-- Query with WHERE number = number (previously caused infinite loop)
SELECT * FROM numbers(18446744073709551611, 2) WHERE number = number;

-- With maximum UInt64 value
SELECT * FROM numbers(18446744073709551615, 1);

-- With offset at max - 1
SELECT * FROM numbers(18446744073709551614, 2);

-- Wraparound case with WHERE - should behave like non-WHERE version
SELECT * FROM numbers(18446744073709551615, 5) WHERE number;

-- Also verify non-WHERE wraparound behavior
SELECT * FROM numbers(18446744073709551615, 5);

-- Wraparound case with WHERE and step
SELECT number FROM numbers(18446744073709551614, 5, 3) WHERE number;
