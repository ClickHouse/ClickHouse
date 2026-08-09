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

SELECT 'bounds between the calendar boundary and the storage boundary of Date32 and DateTime64 are invalid';

-- For Date32 and DateTime64 the values between the calendar boundary and the storage boundary do not wrap, but
-- they are equally invalid: no conversion produces them (they all clamp at the calendar boundary), yet a FROM
-- bound in that gap is materialized into the column as is and serialized as the clamped boundary date - a
-- spurious duplicate of the genuine boundary value next to it. Date32 stores days in Int32 while the calendar
-- covers day numbers [-719528, 2932896]; DateTime64 stores ticks in Int64 while the calendar covers seconds
-- [-62167219200, 253402300799] shifted by the UTC offset of the column's time zone.
SELECT d FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d ASC WITH FILL FROM -719529 TO -719528 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT d FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d DESC WITH FILL FROM 2932897 TO 2932800 STEP INTERVAL -1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT t FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM -62167219201 TO -62167219199 STEP INTERVAL 1 SECOND) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT t FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t DESC WITH FILL FROM 253402300800 TO 253402300700 STEP INTERVAL -1 SECOND) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- The window is taken in the local civil calendar of the column's time zone: local 0000-01-01 00:00:00 in
-- Etc/GMT+12 is -62167176000 in raw ticks, so a FROM below it is rejected even though it fits the UTC window.
SELECT t FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'Etc/GMT+12') AS t ORDER BY t ASC WITH FILL FROM -62167176001 TO -62167175999 STEP INTERVAL 1 SECOND) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- The same materialization happens under a plain numeric step (and with no TO at all), so an out-of-calendar
-- FROM is rejected regardless of the step, like a FROM out of the storage range already is.
SELECT d FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d ASC WITH FILL FROM -719529 TO -719528 STEP 1) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT d FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d ASC WITH FILL FROM -719529) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT d FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d ASC WITH FILL FROM 2932897 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- A numeric step generates out-of-calendar values whenever the sequence crosses the calendar boundary below
-- TO, under the same rules as the storage range: the last generated value has to fit.
SELECT d FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d ASC WITH FILL FROM 2932890 TO 2932900 STEP 1) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT d FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d DESC WITH FILL TO -719600 STEP -1) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- With a larger step the last generated value stays within the calendar and the same TO is accepted.
SELECT count() FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d ASC WITH FILL FROM 2932890 TO 2932900 STEP 10);
-- A TO out of the calendar against the fill direction is a guaranteed no-op and stays accepted.
SELECT count() FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d DESC WITH FILL TO 2932900 STEP -1);
-- FROM bounds at exactly the calendar boundary are generated and terminate.
SELECT count() FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d ASC WITH FILL FROM -719528 TO -719520 STEP INTERVAL 1 YEAR);
SELECT count() FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM -62167219200 TO -62167219100 STEP INTERVAL 1 MINUTE);

SELECT 'a numeric step over DateTime64 must not generate ticks beyond the calendar';

-- The DateTime64 bounds are carried as Decimal64 in the column's scale, and with FROM and a plain numeric step
-- the last generated raw tick is just as knowable as for the Int64-carried types. Ticks beyond the calendar
-- window do not wrap, but they are equally invalid: they all serialize as the clamped boundary date.
SELECT t FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-12-31 23:59:58', 0, 'UTC') TO 253402300805 STEP 2) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT t FROM (SELECT toDateTime64('0000-01-01 00:00:01', 0, 'UTC') AS t ORDER BY t DESC WITH FILL FROM toDateTime64('0000-01-01 00:00:01', 0, 'UTC') TO -62167219202 STEP -2) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- The arithmetic is performed in raw ticks of the column's scale: at scale 3 the step 0.001 is one tick.
SELECT t FROM (SELECT toDateTime64('9999-12-31 23:59:59.998', 3, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-12-31 23:59:59.998', 3, 'UTC') TO 253402300800.001 STEP 0.001) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- Without FROM the bound is rejected only when the last generated value wraps from every possible anchor.
SELECT t FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL TO 253402300805 STEP 2) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- The exclusive TO bound may be one step beyond the calendar boundary: these stop at the last representable tick.
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-12-31 23:59:58', 0, 'UTC') TO 253402300800 STEP 2);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:59.998', 3, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-12-31 23:59:59.998', 3, 'UTC') TO 253402300800 STEP 0.001);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL TO 253402300800 STEP 1);

SELECT 'an INTERVAL step that stagnates at the calendar boundary before an in-range TO is rejected';

-- The calendar arithmetic returns its input unchanged when the result would leave the representable calendar,
-- so the sequence can stop advancing strictly below a perfectly representable TO and never terminate. With an
-- explicit FROM the whole sequence is known up front, and a fill that provably stagnates is rejected.
SELECT t FROM (SELECT toDateTime64('9999-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-06-01 00:00:00', 0, 'UTC') TO toDateTime64('9999-12-31 00:00:00', 0, 'UTC') STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- The stagnation may strike several steps after FROM.
SELECT t FROM (SELECT toDateTime64('9995-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9995-06-01 00:00:00', 0, 'UTC') TO toDateTime64('9999-12-31 00:00:00', 0, 'UTC') STEP INTERVAL 2 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- A TO just above the stagnation point is still unreachable.
SELECT t FROM (SELECT toDateTime64('9998-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9998-06-01 00:00:00', 0, 'UTC') TO toDateTime64('9999-06-02 00:00:00', 0, 'UTC') STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- Date32 stagnates the same way (day number 2932501 is 9998-12-01, below the boundary 2932896 = 9999-12-31).
SELECT d FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d ASC WITH FILL FROM 2932501 TO 2932896 STEP INTERVAL 1 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- A sequence that crosses TO before reaching the calendar boundary terminates and is accepted.
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9998-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9998-06-01 00:00:00', 0, 'UTC') TO toDateTime64('9999-01-01 00:00:00', 0, 'UTC') STEP INTERVAL 1 YEAR);
-- Without FROM the sequence is anchored at a data value and nothing is provable up front.
SELECT count() FROM (SELECT toDateTime64('9999-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t DESC WITH FILL TO toDateTime64('9999-01-01 00:00:00', 0, 'UTC') STEP INTERVAL -1 MONTH);

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
