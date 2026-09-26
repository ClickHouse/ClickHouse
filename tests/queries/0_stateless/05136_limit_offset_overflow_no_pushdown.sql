-- A source that pre-limits itself is told to produce `limit + offset` rows, because the OFFSET is
-- applied on top of what it produces. When that sum does not fit into `UInt64` there is no bound to
-- push down at all, but both ways of computing it used to turn the overflow into a zero:
-- `NumbersLikeUtils::getLimitFromQueryInfo` let the addition wrap around, and
-- `LimitStep::getLimitForSorting` returns 0 as its "unknown" sentinel, which
-- `optimizePrimaryKeyConditionAndLimit` then passed on as an exact bound. The sources below
-- therefore returned nothing at all instead of every row after the offset.

SELECT 'numbers';
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 18446744073709551615 OFFSET 1);
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 18446744073709551614 OFFSET 2);
SELECT count() FROM (SELECT * FROM numbers(10) WHERE number > 3 LIMIT 18446744073709551615 OFFSET 1);

SELECT 'generate_series';
SELECT count() FROM (SELECT * FROM generate_series(1, 10) LIMIT 18446744073709551615 OFFSET 1);
SELECT count() FROM (SELECT * FROM generate_series(10, 0, -1) LIMIT 18446744073709551615 OFFSET 1);

SELECT 'no overflow';
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 18446744073709551615);
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 3 OFFSET 1);
-- `limit + offset` is exactly `18446744073709551615`, the largest value that still fits.
SELECT count() FROM (SELECT * FROM numbers(20) LIMIT 18446744073709551605 OFFSET 10);

-- A real `LIMIT 0` still reads nothing.
SELECT 'limit 0';
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 0);
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 0 OFFSET 3);
