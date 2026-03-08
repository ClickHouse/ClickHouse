-- Test for https://github.com/ClickHouse/ClickHouse/issues/92980
-- Inconsistency in casting of Bool type with IN operator
-- Bool values should correctly match when compared with numeric literals
-- that would convert to the same Bool value

-- Basic tests from the issue: values that convert to true (non-zero) should match CAST(1, 'Bool')
SELECT 'Testing Bool IN operator with various numeric values:';

-- These should all return 1 (true) because all non-zero values convert to Bool true
SELECT CAST(1, 'Bool') IN (10);
SELECT CAST(1, 'Bool') IN (255);
SELECT CAST(1, 'Bool') IN (256);
SELECT CAST(1, 'Bool') IN (10000);

-- This should return 0 (false) because 0 converts to Bool false, not true
SELECT CAST(1, 'Bool') IN (0);

-- Additional edge cases
SELECT 'Testing Bool false (CAST(0)) with IN operator:';
SELECT CAST(0, 'Bool') IN (0);      -- Should be 1 (both are false)
SELECT CAST(0, 'Bool') IN (10);     -- Should be 0 (false vs true)
SELECT CAST(0, 'Bool') IN (255);    -- Should be 0 (false vs true)
SELECT CAST(0, 'Bool') IN (256);    -- Should be 0 (false vs true)

-- Test with multiple values in IN clause
SELECT 'Testing Bool with multiple values in IN:';
SELECT CAST(1, 'Bool') IN (0, 10);      -- Should be 1 (true is in set since 10 -> true)
SELECT CAST(1, 'Bool') IN (0);          -- Should be 0 (true is not in {false})
SELECT CAST(0, 'Bool') IN (0, 10);      -- Should be 1 (false is in set since 0 -> false)
SELECT CAST(0, 'Bool') IN (10, 255);    -- Should be 0 (false not in {true, true})

-- Test with negative numbers (all non-zero, should convert to true)
SELECT 'Testing Bool with negative numbers:';
SELECT CAST(1, 'Bool') IN (-1);         -- Should be 1 (true matches true)
SELECT CAST(1, 'Bool') IN (-255);       -- Should be 1 (true matches true)
SELECT CAST(0, 'Bool') IN (-1);         -- Should be 0 (false does not match true)

-- Test materialized columns (not just constants)
SELECT 'Testing Bool with materialized values:';
SELECT CAST(materialize(1), 'Bool') IN (10);   -- Should be 1
SELECT CAST(materialize(1), 'Bool') IN (255);  -- Should be 1
SELECT CAST(materialize(0), 'Bool') IN (0);    -- Should be 1

-- Test Nullable(Bool) - values wrapped in Nullable should also convert correctly
SELECT 'Testing Nullable(Bool) with IN operator:';
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(10));    -- Should be 1
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(255));   -- Should be 1
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(256));   -- Should be 1
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(1000));  -- Should be 1
SELECT CAST(0, 'Nullable(Bool)') IN (toNullable(0));     -- Should be 1
SELECT CAST(0, 'Nullable(Bool)') IN (toNullable(10));    -- Should be 0

-- Test LowCardinality(Bool) - values wrapped in LowCardinality should also convert correctly
SELECT 'Testing LowCardinality(Bool) with IN operator:';
SELECT toLowCardinality(CAST(1, 'Bool')) IN (10);   -- Should be 1
SELECT toLowCardinality(CAST(1, 'Bool')) IN (255);  -- Should be 1
SELECT toLowCardinality(CAST(1, 'Bool')) IN (256);  -- Should be 1
SELECT toLowCardinality(CAST(0, 'Bool')) IN (0);    -- Should be 1
SELECT toLowCardinality(CAST(0, 'Bool')) IN (10);   -- Should be 0
