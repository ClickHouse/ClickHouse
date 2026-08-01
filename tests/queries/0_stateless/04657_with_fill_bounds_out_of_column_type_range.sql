-- WITH FILL bound values that do not fit the type of the ORDER BY column used to be silently truncated when the
-- generated values were written into the column, so the filled stream wrapped around and was not sorted anymore,
-- while the query plan kept claiming it was. DISTINCT in order then deduplicated within wrong ranges.

SELECT 'out of range bounds are rejected';

SELECT * FROM (SELECT 5 AS x ORDER BY x ASC WITH FILL FROM 1 TO 1025) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL FROM 1000 TO 1010) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toUInt8(5) AS x ORDER BY x DESC WITH FILL FROM 300 TO 250) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toInt8(5) AS x ORDER BY x ASC WITH FILL FROM 120 TO 140) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDate(0) AS d ORDER BY d ASC WITH FILL FROM 0 TO 100000) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDate32('2020-01-01') AS d ORDER BY d ASC WITH FILL FROM 0 TO 3000000000) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime(5, 'UTC') AS t ORDER BY t ASC WITH FILL FROM 0 TO 4294967297) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL FROM 0 TO 260 STEP 3) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'the exclusive TO bound may be one step outside of the range';

SELECT count(), min(x), max(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL FROM 0 TO 256);
SELECT count(), min(x), max(x) FROM (SELECT toInt8(-5) AS x ORDER BY x DESC WITH FILL FROM 0 TO -129);
SELECT count(), min(x), max(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 0);

SELECT 'a TO bound out of range is fine when STEP stops before it';

SELECT count(), min(x), max(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL FROM 0 TO 257 STEP 3);
SELECT count(), min(x), max(x) FROM (SELECT toInt8(5) AS x ORDER BY x DESC WITH FILL FROM 127 TO -130 STEP -3);
SELECT groupArray(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL FROM 250 TO 300 STEP 100);
SELECT * FROM (SELECT toInt8(5) AS x ORDER BY x DESC WITH FILL FROM 127 TO -130 STEP -4) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- Without FROM the sequence is anchored at a data value, so only the value right before TO can be assumed.
SELECT * FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 257 STEP 3) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'in-range filling is unchanged';

SELECT groupArray(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL FROM 1 TO 10);
SELECT groupArray(x) FROM (SELECT toInt8(-5) AS x ORDER BY x DESC WITH FILL FROM -1 TO -8);
SELECT count(), min(d), max(d) FROM (SELECT toDate('2026-01-01') AS d ORDER BY d ASC WITH FILL FROM toDate('2025-12-30') TO toDate('2026-01-03'));
SELECT count(), min(d), max(d) FROM (SELECT toDate32('2026-01-01') AS d ORDER BY d ASC WITH FILL FROM toDate32('2025-12-30') TO toDate32('2026-01-03'));
SELECT count(), min(t), max(t) FROM (SELECT toDateTime('2020-06-16 03:00:00', 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime('2020-06-16 00:00:00', 'UTC') TO toDateTime('2020-06-16 10:00:00', 'UTC') STEP 1800);
SELECT groupArray(x) FROM (SELECT 5.5::Float32 AS x ORDER BY x ASC WITH FILL FROM 5 TO 6 STEP 0.25);

SELECT 'DISTINCT in order over a filled stream';

SET optimize_distinct_in_order = 1;
SELECT count() FROM (SELECT DISTINCT x, s FROM (SELECT toUInt8(5) AS x, 'Hello' AS s ORDER BY x ASC WITH FILL FROM 0 TO 256));
SET optimize_distinct_in_order = 0;
SELECT count() FROM (SELECT DISTINCT x, s FROM (SELECT toUInt8(5) AS x, 'Hello' AS s ORDER BY x ASC WITH FILL FROM 0 TO 256));

-- The query the AST fuzzer aborted on: the second UNION ALL branch filled a UInt8 column up to 1025.
SELECT DISTINCT x, isZeroOrNull(materialize(true)), s
FROM
(
    SELECT 5 AS x, 'Hello' AS s ORDER BY x ASC NULLS LAST WITH FILL FROM 1 TO 10 INTERPOLATE (`s` AS concat(s, 'A')) LIMIT 1048576
    UNION ALL
    SELECT 5 AS x, 'Hello' AS s ORDER BY x ASC NULLS LAST WITH FILL FROM 1 TO 1025 INTERPOLATE (`s` AS concatAssumeInjective(s, 'A')) LIMIT 1048576
)
ORDER BY s ASC
FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
