-- Checks of window definitions and modifiers that are inconsistent today.
-- This test pins the current behaviour so that a fix has to update the reference.

SELECT '-- A window function inside PARTITION BY or ORDER BY of an inline window is not rejected during analysis';
-- The query fails later with a missing column. The same in a WINDOW clause is rejected with ILLEGAL_AGGREGATION,
-- which is the error the inline form should give as well.
SELECT count() OVER (PARTITION BY row_number() OVER (ORDER BY number)) FROM numbers(3); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
SELECT count() OVER (ORDER BY row_number() OVER (ORDER BY number)) FROM numbers(3); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
SELECT count() OVER (PARTITION BY number % 2 ORDER BY sum(number) OVER (ORDER BY number)) FROM numbers(3); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
SELECT count() OVER w FROM numbers(3) WINDOW w AS (PARTITION BY row_number() OVER (ORDER BY number)); -- { serverError ILLEGAL_AGGREGATION }
SELECT count() OVER w FROM numbers(3) WINDOW w AS (ORDER BY row_number() OVER (ORDER BY number)); -- { serverError ILLEGAL_AGGREGATION }

SELECT '-- The largest 32-bit offset, 2147483647, is rejected although the message asks for a 32-bit integer; 2147483646 is accepted';
SELECT count() OVER (ORDER BY number ROWS BETWEEN 2147483647 PRECEDING AND CURRENT ROW) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND 2147483647 FOLLOWING) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY number RANGE BETWEEN 2147483647 PRECEDING AND CURRENT ROW) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (ORDER BY number GROUPS BETWEEN 2147483647 PRECEDING AND CURRENT ROW) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(c) FROM (SELECT count() OVER (ORDER BY number ROWS BETWEEN 2147483646 PRECEDING AND 2147483646 FOLLOWING) AS c FROM numbers(3));

SELECT '-- IGNORE NULLS is silently accepted on functions that do not pick a value, while RESPECT NULLS is rejected for them';
SELECT number,
    row_number() IGNORE NULLS OVER w AS rn, rank() IGNORE NULLS OVER w AS r, dense_rank() IGNORE NULLS OVER w AS dr,
    lag(number) IGNORE NULLS OVER w AS l, lead(number) IGNORE NULLS OVER w AS ld, nth_value(number, 1) IGNORE NULLS OVER w AS nv, ntile(2) IGNORE NULLS OVER w AS nt
FROM numbers(3) WINDOW w AS (ORDER BY number) ORDER BY number;
SELECT row_number() RESPECT NULLS OVER (ORDER BY number) FROM numbers(3); -- { serverError NOT_IMPLEMENTED }
SELECT lag(number) RESPECT NULLS OVER (ORDER BY number) FROM numbers(3); -- { serverError NOT_IMPLEMENTED }
