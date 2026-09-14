-- A numeric literal too large for UInt64 resolves to a wide integer, and a float-valued setting has
-- to take it as a number. It used to arrive as Float64, so rejecting it breaks queries that worked.

SET totals_auto_threshold = 18446744073709551616;
SELECT value FROM system.settings WHERE name = 'totals_auto_threshold';

SET totals_auto_threshold = -18446744073709551616;
SELECT value FROM system.settings WHERE name = 'totals_auto_threshold';

SET max_rand_distribution_parameter = 18446744073709551616;
SELECT value FROM system.settings WHERE name = 'max_rand_distribution_parameter';

-- The exponent spelling of the same value resolves identically.
SET totals_auto_threshold = 1.8446744073709552e19;
SELECT value FROM system.settings WHERE name = 'totals_auto_threshold';

-- An integer setting still refuses a value out of its range instead of truncating it silently.
SET max_threads = 18446744073709551616; -- { serverError CANNOT_CONVERT_TYPE }
