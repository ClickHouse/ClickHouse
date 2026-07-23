-- Tags: long
-- Exercise the parallel two-level DISTINCT build for the string key families
-- (`key_string` and `key_fixed_string`). Lowering `distinct_two_level_threshold`
-- to 1000 forces promotion to two-level so the per-bucket parallel build runs;
-- the results MUST match the serial path (threshold 0 disables promotion).

SET max_threads = 8;

-- key_string: variable-length String
SET distinct_two_level_threshold = 1000;
SELECT count() FROM (SELECT DISTINCT toString(number % 400000) AS k FROM numbers_mt(4000000));
SET distinct_two_level_threshold = 0;
SELECT count() FROM (SELECT DISTINCT toString(number % 400000) AS k FROM numbers_mt(4000000));

-- key_fixed_string: FixedString
SET distinct_two_level_threshold = 1000;
SELECT count() FROM (SELECT DISTINCT toFixedString(toString(number % 200000), 12) AS k FROM numbers_mt(4000000));
SET distinct_two_level_threshold = 0;
SELECT count() FROM (SELECT DISTINCT toFixedString(toString(number % 200000), 12) AS k FROM numbers_mt(4000000));

-- key_string again, with a longer prefixed value to stress arena persistence
SET distinct_two_level_threshold = 1000;
SELECT count() FROM (SELECT DISTINCT concat('p', toString(number % 350000)) AS k FROM numbers_mt(4000000));
SET distinct_two_level_threshold = 0;
SELECT count() FROM (SELECT DISTINCT concat('p', toString(number % 350000)) AS k FROM numbers_mt(4000000));

-- Verify the actual distinct string VALUES survive intact (arena-backed keys must
-- not dangle): a content-sensitive digest of the distinct set built with promotion
-- must equal the digest built without it. Emitted as a single boolean so no magic
-- constant is needed.
SELECT
(
    SELECT sum(cityHash64(k)) FROM (SELECT DISTINCT concat('prefix_', toString(number % 100000)) AS k FROM numbers_mt(2000000)) SETTINGS distinct_two_level_threshold = 1000
) = (
    SELECT sum(cityHash64(k)) FROM (SELECT DISTINCT concat('prefix_', toString(number % 100000)) AS k FROM numbers_mt(2000000)) SETTINGS distinct_two_level_threshold = 0
);
