-- Tags: no-fasttest
-- Test that extremely large max_execution_time values don't cause livelock in CancellationChecker.
-- The timeout is internally capped to 1 year to prevent overflow in std::condition_variable::wait_for.
-- This query should complete quickly without hanging, regardless of the huge timeout value.

SET max_execution_time = 9223372041;  -- Close to INT64_MAX / 1000000000, would overflow when converted to nanoseconds
SELECT 1;
SELECT 1;
SELECT 1;
