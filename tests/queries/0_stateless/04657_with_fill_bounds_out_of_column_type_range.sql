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

SELECT 'without FROM the bound is rejected only when every anchor wraps';

-- The sequence is anchored at a data value, so which values are generated is known only at execution time:
-- from 5 this fill stops at 254, while from 6 it would reach 256. The bound is accepted because some anchors fit.
SELECT count(), min(x), max(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 257 STEP 3);
SELECT count(), min(x), max(x) FROM (SELECT toInt8(-5) AS x ORDER BY x DESC WITH FILL TO -130 STEP -5);
-- A step so large that filling generates nothing at all is fine too.
SELECT groupArray(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1000 STEP 5000);
-- The last generated value always lands within one step before TO, so these wrap from every possible anchor.
SELECT * FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1025) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1000 STEP 3) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toInt8(-5) AS x ORDER BY x DESC WITH FILL TO -300 STEP -5) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'STALENESS caps the sequence before TO, so the TO bound is not rejected';

-- STALENESS is allowed only without FROM, and it replaces TO as the effective bound whenever it comes first,
-- so the sequence stops at the last data value plus the staleness and stays far below an out of range TO.
SELECT count(), min(x), max(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1025 STALENESS 20);
SELECT count(), min(x), max(x) FROM (SELECT toInt8(-5) AS x ORDER BY x DESC WITH FILL TO -300 STALENESS -20);
SELECT count(), min(x), max(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1000 STEP 3 STALENESS 21);

SELECT 'an INTERVAL step can never reach a TO out of range in the fill direction, so such a TO is rejected';

-- The calendar arithmetic of an INTERVAL step is performed in the column's own native type, so unlike a plain
-- numeric step it wraps around within the column domain and never reaches a TO outside of it: without the check
-- these fills generate wrapped-around values forever.
SELECT * FROM (SELECT toDate(0) AS d ORDER BY d ASC WITH FILL FROM toDate(0) TO 70000 STEP INTERVAL 100 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDate('1970-03-05') AS d ORDER BY d ASC WITH FILL TO 70000 STEP INTERVAL 100 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL TO -5 STEP INTERVAL -1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime(0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime(0, 'UTC') TO 4294967297 STEP INTERVAL 50 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- A TO out of range against the fill direction can never make filling take a single step: every possible
-- anchor is already past it, so the query is a no-op and the bound is accepted.
SELECT count(), min(d), max(d) FROM (SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL TO 70000 STEP INTERVAL -1 YEAR);
SELECT count(), min(d), max(d) FROM (SELECT toDate('2020-01-01') AS d ORDER BY d ASC WITH FILL TO -5 STEP INTERVAL 1 YEAR);
-- STALENESS terminates the filling in-domain even with an INTERVAL step, so the TO bound is accepted.
SELECT count(), min(d), max(d) FROM (SELECT toDate('2026-03-05') AS d ORDER BY d ASC WITH FILL TO 70000 STEP INTERVAL 1 YEAR STALENESS INTERVAL 3 YEAR);

SELECT 'an INTERVAL step clamps at the calendar boundary, which for Date32 and DateTime64 is inside the storage range';

-- The calendar arithmetic of an INTERVAL step clamps at the representable calendar, [0000-01-01, 9999-12-31],
-- and for Date32 and DateTime64 that window is strictly narrower than the storage type: a TO bound beyond the
-- calendar boundary fits the storage type but can never be reached, so without the check these fills keep
-- generating the clamped boundary value forever.
SELECT * FROM (SELECT toDate32('9999-12-31') AS d ORDER BY d ASC WITH FILL TO 3000000 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDate32('9999-06-01') AS d ORDER BY d ASC WITH FILL TO 2932897 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDate32('0001-06-01') AS d ORDER BY d DESC WITH FILL TO -800000 STEP INTERVAL -1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime64('9999-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL TO 253402300800 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime64('9999-06-01 00:00:00.123', 3, 'UTC') AS t ORDER BY t ASC WITH FILL TO 253402300800 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime64('0001-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t DESC WITH FILL TO -70000000000 STEP INTERVAL -100 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- The calendar clamp happens in the local civil calendar of the column's time zone, so the boundary expressed
-- in raw ticks is shifted by the UTC offset: local 9999-12-31 23:59:59 is 253402250399 in Etc/GMT-14 (UTC+14)
-- and 253402343999 in Etc/GMT+12 (UTC-12), while local 0000-01-01 00:00:00 in Etc/GMT+12 is -62167176000.
-- A TO within the UTC calendar window but beyond the local one can never be reached.
SELECT * FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'Etc/GMT-14') AS t ORDER BY t ASC WITH FILL TO 253402250400 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime64('2000-01-01 00:00:00.123', 3, 'Etc/GMT-14') AS t ORDER BY t ASC WITH FILL TO 253402250400000 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime64('0001-06-01 00:00:00', 0, 'Etc/GMT+12') AS t ORDER BY t DESC WITH FILL TO -62167219200 STEP INTERVAL -100 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime64('9998-06-01 00:00:00', 0, 'Etc/GMT+12') AS t ORDER BY t ASC WITH FILL TO 253402344000 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- A TO beyond the UTC calendar window but within the local one is accepted (the anchor steps exactly onto the
-- exclusive bound, so the fill terminates with the anchor row alone).
SELECT count() FROM (SELECT toDateTime64('9998-12-31 23:59:59', 0, 'Etc/GMT+12') AS t ORDER BY t ASC WITH FILL TO 253402343999 STEP INTERVAL 1 YEAR);
-- The exclusive TO bound at exactly the calendar boundary is reachable and terminates. Only acceptance and
-- termination are asserted: an anchor beyond the DateLUT table takes the out-of-range calendar path, whose
-- clamped values are a pre-existing data-dependent artifact (see the pull request description).
SELECT count() > 0 FROM (SELECT toDate32('9995-06-01') AS d ORDER BY d ASC WITH FILL TO 2932896 STEP INTERVAL 1 YEAR);
-- In-range INTERVAL fills over Date32 and DateTime64 are unchanged.
SELECT count(), min(d), max(d) FROM (SELECT toDate32('2026-01-01') AS d ORDER BY d ASC WITH FILL FROM toDate32('2020-01-01') TO toDate32('2027-01-01') STEP INTERVAL 1 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('2020-01-03 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('2020-01-01 00:00:00', 0, 'UTC') TO toDateTime64('2020-01-05 00:00:00', 0, 'UTC') STEP INTERVAL 1 DAY);
-- STALENESS terminates the filling in-domain, so an out-of-calendar TO is accepted.
SELECT count(), min(d), max(d) FROM (SELECT toDate32('2026-03-05') AS d ORDER BY d ASC WITH FILL TO 3000000 STEP INTERVAL 1 YEAR STALENESS INTERVAL 3 YEAR);

SELECT 'in-range filling is unchanged';

SELECT groupArray(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL FROM 1 TO 10);
SELECT groupArray(x) FROM (SELECT toInt8(-5) AS x ORDER BY x DESC WITH FILL FROM -1 TO -8);
SELECT count(), min(d), max(d) FROM (SELECT toDate('2026-01-01') AS d ORDER BY d ASC WITH FILL FROM toDate('2025-12-30') TO toDate('2026-01-03'));
SELECT count(), min(d), max(d) FROM (SELECT toDate('2026-01-01') AS d ORDER BY d ASC WITH FILL FROM toDate('2020-01-01') TO toDate('2027-01-01') STEP INTERVAL 1 YEAR);
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
