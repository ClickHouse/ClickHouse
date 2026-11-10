-- Test for GLOBAL IN with nullable subquery types
-- This test verifies that external tables for GLOBAL IN work correctly when the
-- subquery returns Nullable types. The Set class strips Nullable from types when
-- storing elements (if transform_null_in is false), but the external table must
-- use the original Nullable types. When the set is built first (before streaming
-- to the external table), the set elements must be converted back to Nullable
-- before being written to the external table.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab0;

CREATE TABLE tab0 (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO tab0 SELECT number, number FROM numbers(100);

-- Test GLOBAL IN with nullable subquery (materialize(toNullable(...)))
-- This used to crash because when building external table from a ready set,
-- the set elements (non-nullable) were written directly without converting
-- them to match the external table's expected Nullable types.
SELECT sum(y) FROM (SELECT * FROM remote('127.0.0.{1,2}', currentDatabase(), tab0)) WHERE x GLOBAL IN (SELECT materialize(toNullable(42)) + number FROM numbers(1));

-- Additional test cases
SELECT sum(y) FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), tab0)) WHERE x GLOBAL IN (SELECT toNullable(number) FROM numbers(5));

DROP TABLE tab0;
