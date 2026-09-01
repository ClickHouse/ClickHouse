-- Test for tuple IN set - regression test for LOGICAL_ERROR: 'Expected Tuple or Nullable(Tuple) type'
-- The fix handles edge cases in type checking and provides user-friendly errors instead of crashing

-- This query reproduces the original bug (LOGICAL_ERROR in debug builds)
SELECT (1, 2) IN [(1, 2), toLowCardinality(1), NULL] SETTINGS enable_analyzer = 1; -- { serverError INCORRECT_ELEMENT_OF_SET }

SELECT (1, 2) IN [(1, 2), toLowCardinality(1), NULL] SETTINGS enable_analyzer = 0; -- { serverError NO_COMMON_TYPE }
  
-- Test tuple IN tuple with mixed element types (scalar instead of tuple)
SELECT (1, 2) IN ((1, 2), 1); -- { serverError INCORRECT_ELEMENT_OF_SET }

-- Valid cases should still work
SELECT (1, 2) IN [(1, 2), (3, 4)];
SELECT (1, 2) IN ((1, 2), (3, 4));
SELECT tuple(1, 2) IN (tuple(1, 2), tuple(3, 4));

-- Nullable tuple cases (requires experimental setting)
SET allow_experimental_nullable_tuple_type = 1;
SELECT (1, 2)::Nullable(Tuple(Int32, Int32)) IN ((1, 2), (3, 4));
SELECT (1, 2)::Nullable(Tuple(Int32, Int32)) IN ((1, 2), NULL);
