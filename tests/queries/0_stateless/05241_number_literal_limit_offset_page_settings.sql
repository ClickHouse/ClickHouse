-- A query-level `limit` / `offset` / `page` setting on a subquery is read straight from the parsed
-- literal. A value too large for UInt64 arrives as a wide integer and is still a number: it fails the
-- same range check as the equivalent Float64 spelling instead of being rejected as a non-numeric value.

SELECT * FROM (SELECT number FROM numbers(3) SETTINGS limit = 18446744073709551616); -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT * FROM (SELECT number FROM numbers(3) SETTINGS limit = 1.8446744073709552e19); -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT * FROM (SELECT number FROM numbers(3) SETTINGS limit = 2, offset = 18446744073709551616); -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT * FROM (SELECT number FROM numbers(3) SETTINGS limit = 1, page = 18446744073709551616); -- { serverError INVALID_LIMIT_EXPRESSION }

SELECT * FROM (SELECT number FROM numbers(3) SETTINGS limit = 2, offset = 1);
