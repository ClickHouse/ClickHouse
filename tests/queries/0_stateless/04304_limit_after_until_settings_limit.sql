-- The `limit`/`offset` settings are a global cap on the whole result, not a per-window length.
-- With LIMIT AFTER ... ALL they must not be folded into the per-window length; instead they are
-- applied as an outer LIMIT/OFFSET after the range step. Checked on both the analyzer and legacy paths.

SET allow_experimental_limit_after = 1;

-- limit setting with AFTER ... ALL and no explicit window length: global cap of 2 rows.
SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number IN (1, 5) ALL SETTINGS limit = 2;
SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number IN (1, 5) ALL SETTINGS limit = 2, enable_analyzer = 0;

-- explicit per-window length 3 stays the window length; the limit setting caps the whole result to 2.
SELECT number FROM numbers(20) ORDER BY number LIMIT 3 AFTER number IN (1, 5) ALL SETTINGS limit = 2;
SELECT number FROM numbers(20) ORDER BY number LIMIT 3 AFTER number IN (1, 5) ALL SETTINGS limit = 2, enable_analyzer = 0;

-- without the setting the per-window length is unchanged (1 2 3 from each of the two windows).
SELECT number FROM numbers(20) ORDER BY number LIMIT 3 AFTER number IN (1, 5) ALL;
SELECT number FROM numbers(20) ORDER BY number LIMIT 3 AFTER number IN (1, 5) ALL SETTINGS enable_analyzer = 0;

-- offset setting acts as a global skip and limit setting as a global cap, applied after the range.
SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number >= 3 SETTINGS limit = 2, offset = 1;
SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number >= 3 SETTINGS limit = 2, offset = 1, enable_analyzer = 0;

-- the setting must cap the final outer result; it must NOT leak into the inner subquery (which would
-- truncate it to LIMIT 2 and make the outer AFTER number >= 5 never match). Expect 5, 6 on both paths.
SELECT number FROM (SELECT number FROM numbers(10)) ORDER BY number LIMIT AFTER number >= 5 SETTINGS limit = 2;
SELECT number FROM (SELECT number FROM numbers(10)) ORDER BY number LIMIT AFTER number >= 5 SETTINGS limit = 2, enable_analyzer = 0;
