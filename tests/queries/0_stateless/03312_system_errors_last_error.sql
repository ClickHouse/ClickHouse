-- Tags: no-parallel
-- Tag no-parallel: The test checks system.errors values which are global

-- For the old analyzer last_error_message is slightly different.
SET enable_analyzer = 1;

SELECT throwIf(1); -- {serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO}

-- We expect an extended error message here like "Value passed to 'throwIf' function is non-zero: while executing throwIf(1)",
-- and not just "Value passed to 'throwIf' function is non-zero".
SELECT last_error_message, last_error_format_string FROM system.errors
WHERE name = 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' AND last_error_time > now() - 10 AND not remote;
