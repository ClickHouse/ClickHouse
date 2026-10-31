-- WITH FILL bound values that do not fit the type of the ORDER BY column used to be silently truncated when the
-- generated values were written into the column, so the filled stream wrapped around and was not sorted anymore,
-- while the query plan kept claiming it was. DISTINCT in order then deduplicated within wrong ranges.
-- A fill with FROM generates the same sequence for any data, so it is checked up front; a fill anchored at a data
-- value is checked value by value as it is generated.

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

SELECT 'without FROM the query is rejected only when the fill actually wraps';

-- The sequence is anchored at a data value, so which values are generated is known only at execution time:
-- from 5 this fill stops at 254, while from 6 it would reach 256.
SELECT count(), min(x), max(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 257 STEP 3);
SELECT count(), min(x), max(x) FROM (SELECT toInt8(-5) AS x ORDER BY x DESC WITH FILL TO -130 STEP -5);
-- A step so large that filling generates nothing at all is fine too.
SELECT groupArray(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1000 STEP 5000);
-- The last generated value always lands within one step before TO, so these wrap from every possible anchor...
SELECT * FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1025) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1000 STEP 3) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toInt8(-5) AS x ORDER BY x DESC WITH FILL TO -300 STEP -5) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- ...but without any anchor nothing is generated at all, so nothing wraps and the bound is harmless.
SELECT count() FROM (SELECT x FROM (SELECT toUInt8(1) AS x WHERE 0) ORDER BY x WITH FILL TO 1025);
SELECT count() FROM (SELECT d FROM (SELECT toDate(0) AS d WHERE 0) ORDER BY d WITH FILL TO 70000 STEP INTERVAL 100 YEAR);
-- With FROM the sequence is generated even without data, so an empty input does not help.
SELECT count() FROM (SELECT x FROM (SELECT toUInt8(1) AS x WHERE 0) ORDER BY x WITH FILL FROM 0 TO 1025); -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'STALENESS caps the sequence before TO, so the TO bound is not rejected';

-- STALENESS is allowed only without FROM, and it replaces TO as the effective bound whenever it comes first,
-- so the sequence stops at the last data value plus the staleness and stays far below an out of range TO.
SELECT count(), min(x), max(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1025 STALENESS 20);
SELECT count(), min(x), max(x) FROM (SELECT toInt8(-5) AS x ORDER BY x DESC WITH FILL TO -300 STALENESS -20);
SELECT count(), min(x), max(x) FROM (SELECT toUInt8(5) AS x ORDER BY x ASC WITH FILL TO 1000 STEP 3 STALENESS 21);
-- The border by itself is not validated: a border past the range of the column type is fine when TO caps the
-- fill inside the range before any value the column cannot hold is generated.
SELECT groupArray(x) FROM (SELECT toUInt8(250) AS x ORDER BY x ASC WITH FILL TO 255 STALENESS 20);
-- A no-op STALENESS 0 border equals the anchor value and simply generates nothing.
SELECT groupArray(x) FROM (SELECT toUInt8(250) AS x ORDER BY x ASC WITH FILL STALENESS 0);
-- The effective bound is derived at runtime from the previous input row. When it exceeds the range of the
-- column type, the fill towards it reaches values the column cannot hold, and the per-generated-value check
-- rejects the query before a wrapped bound could suppress the rows between the two input values.
SELECT groupArray(x) FROM (SELECT arrayJoin([toUInt8(250), toUInt8(255)]) AS x ORDER BY x ASC WITH FILL STALENESS 20) SETTINGS max_block_size = 1; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'an INTERVAL step over Date and DateTime is rejected when it leaves the column type before reaching a TO out of range in the fill direction';

-- The calendar arithmetic of an INTERVAL step over Date and DateTime does not stop near the boundary of the
-- column type, so the sequence passes that boundary when its next step is still before a TO outside of it (without
-- the check these fills used to wrap around and generate values forever). When the step past the boundary is not
-- before TO, the filling ends there instead.
SELECT * FROM (SELECT toDate(0) AS d ORDER BY d ASC WITH FILL FROM toDate(0) TO 80000 STEP INTERVAL 100 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime(0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime(0, 'UTC') TO 5000000000 STEP INTERVAL 50 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT groupArray(d) FROM (SELECT toDate(0) AS d ORDER BY d ASC WITH FILL FROM toDate(0) TO 70000 STEP INTERVAL 100 YEAR);
SELECT groupArray(t) FROM (SELECT toDateTime(0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime(0, 'UTC') TO 4294967297 STEP INTERVAL 50 YEAR);
-- Without FROM the fill is checked as it is generated: it is rejected when it generates a value past the boundary
-- of the type before TO...
SELECT * FROM (SELECT toDate('1970-03-05') AS d ORDER BY d ASC WITH FILL TO 70000 STEP INTERVAL 10 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime(0, 'UTC') AS t ORDER BY t ASC WITH FILL TO 5000000000 STEP INTERVAL 150 YEAR) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- ...and it stops when its next value is past TO, even when that value is past the boundary of the type as well.
SELECT count(), min(d), max(d) FROM (SELECT toDate('1970-03-05') AS d ORDER BY d ASC WITH FILL TO 70000 STEP INTERVAL 100 YEAR);
SELECT count(), min(d), max(d) FROM (SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL TO -5 STEP INTERVAL -1 YEAR);
-- A small step towards a TO far out of range is rejected without generating every value up to the boundary of
-- the type first (2^32 rows for DateTime).
SELECT * FROM (SELECT toDateTime(5, 'UTC') AS t ORDER BY t ASC WITH FILL TO 4294967297 STEP INTERVAL 1 SECOND) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime(5, 'UTC') AS t ORDER BY t ASC WITH FILL TO 4294967297) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT * FROM (SELECT toDateTime(4294967290, 'UTC') AS t ORDER BY t DESC WITH FILL TO -2 STEP INTERVAL -1 SECOND) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- A TO out of range against the fill direction can never make filling take a single step: every possible
-- anchor is already past it, so the query is a no-op.
SELECT count(), min(d), max(d) FROM (SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL TO 70000 STEP INTERVAL -1 YEAR);
SELECT count(), min(d), max(d) FROM (SELECT toDate('2020-01-01') AS d ORDER BY d ASC WITH FILL TO -5 STEP INTERVAL 1 YEAR);
-- STALENESS terminates the filling in-domain even with an INTERVAL step, so the TO bound is never reached.
SELECT count(), min(d), max(d) FROM (SELECT toDate('2026-03-05') AS d ORDER BY d ASC WITH FILL TO 70000 STEP INTERVAL 1 YEAR STALENESS INTERVAL 3 YEAR);

SELECT 'an INTERVAL step clamps at the calendar boundary of Date32 and DateTime64, so any TO is accepted';

-- The calendar arithmetic of an INTERVAL step clamps at the representable calendar, [0000-01-01, 9999-12-31],
-- which for Date32 and DateTime64 is strictly narrower than the storage type: a step that would leave the
-- calendar returns its input unchanged, and a step that no longer advances the value ends the filling. So a TO
-- beyond the calendar boundary is accepted, and the fill stops at the last value it can reach.
SELECT count(), min(d), max(d) FROM (SELECT toDate32('9999-12-31') AS d ORDER BY d ASC WITH FILL TO 3000000 STEP INTERVAL 1 YEAR);
SELECT count(), min(d), max(d) FROM (SELECT toDate32('9999-06-01') AS d ORDER BY d ASC WITH FILL TO 2932897 STEP INTERVAL 1 YEAR);
SELECT count(), min(d), max(d) FROM (SELECT toDate32('0001-06-01') AS d ORDER BY d DESC WITH FILL TO -800000 STEP INTERVAL -1 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL TO 253402300800 STEP INTERVAL 1 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-06-01 00:00:00.123', 3, 'UTC') AS t ORDER BY t ASC WITH FILL TO 253402300800 STEP INTERVAL 1 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('0001-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t DESC WITH FILL TO -70000000000 STEP INTERVAL -100 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'Etc/GMT-14') AS t ORDER BY t ASC WITH FILL TO 253402250400 STEP INTERVAL 1000 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9998-06-01 00:00:00', 0, 'Etc/GMT+12') AS t ORDER BY t ASC WITH FILL TO 253402344000 STEP INTERVAL 1 YEAR);
-- The exclusive TO bound at exactly the calendar boundary is reachable and terminates.
SELECT count() > 0 FROM (SELECT toDate32('9995-12-31') AS d ORDER BY d ASC WITH FILL TO 2932896 STEP INTERVAL 1 YEAR);
-- In-range INTERVAL fills over Date32 and DateTime64 are unchanged.
SELECT count(), min(d), max(d) FROM (SELECT toDate32('2026-01-01') AS d ORDER BY d ASC WITH FILL FROM toDate32('2020-01-01') TO toDate32('2027-01-01') STEP INTERVAL 1 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('2020-01-03 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('2020-01-01 00:00:00', 0, 'UTC') TO toDateTime64('2020-01-05 00:00:00', 0, 'UTC') STEP INTERVAL 1 DAY);
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
-- A numeric bound of a DateTime64 key is converted to the key type as CAST converts it, following
-- date_time_overflow_behavior: by default a bound beyond the calendar is clamped to its boundary, with 'throw' it is rejected.
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM -62167219201 TO -62167219199 STEP INTERVAL 1 SECOND);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t DESC WITH FILL FROM 253402300800 TO 253402300700 STEP INTERVAL -1 SECOND);
SELECT t FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM -62167219201 TO -62167219199 STEP INTERVAL 1 SECOND) FORMAT Null SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT t FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t DESC WITH FILL FROM 253402300800 TO 253402300700 STEP INTERVAL -1 SECOND) FORMAT Null SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
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

-- A numeric TO beyond the calendar is clamped to its last tick (see above), so the fill stops within the calendar;
-- with date_time_overflow_behavior = 'throw' such a TO is rejected. The exclusive TO bound one step beyond the
-- calendar boundary is clamped as well, so the last tick of the calendar itself is not generated.
-- The arithmetic is performed in raw ticks of the column's scale: at scale 3 the step 0.001 is one tick.
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-12-31 23:59:58', 0, 'UTC') TO 253402300805 STEP 2);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64(-62167219199, 0, 'UTC') AS t ORDER BY t DESC WITH FILL FROM toDateTime64(-62167219199, 0, 'UTC') TO -62167219202 STEP -2);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:59.998', 3, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-12-31 23:59:59.998', 3, 'UTC') TO 253402300800.001 STEP 0.001);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL TO 253402300805 STEP 2);
SELECT t FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL TO 253402300805 STEP 2) FORMAT Null SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-12-31 23:59:58', 0, 'UTC') TO 253402300800 STEP 2);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:59.998', 3, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-12-31 23:59:59.998', 3, 'UTC') TO 253402300800 STEP 0.001);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL TO 253402300800 STEP 1);

SELECT 'an INTERVAL step that stagnates at the calendar boundary before an in-range TO stops there';

-- The calendar arithmetic returns its input unchanged when the result would leave the representable calendar,
-- so the sequence can stop advancing strictly below a perfectly representable TO: the filling ends there.
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9999-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9999-06-01 00:00:00', 0, 'UTC') TO toDateTime64('9999-12-31 00:00:00', 0, 'UTC') STEP INTERVAL 1 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9995-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9995-06-01 00:00:00', 0, 'UTC') TO toDateTime64('9999-12-31 00:00:00', 0, 'UTC') STEP INTERVAL 2 YEAR);
SELECT count(), min(d), max(d) FROM (SELECT toDate32('2000-01-01') AS d ORDER BY d ASC WITH FILL FROM 2932501 TO 2932896 STEP INTERVAL 1 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9990-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9990-01-01 00:00:00', 0, 'UTC') TO toDateTime64('9999-12-31 23:59:59', 0, 'UTC') STEP INTERVAL 1 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime64('9998-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime64('9998-06-01 00:00:00', 0, 'UTC') TO toDateTime64('9999-01-01 00:00:00', 0, 'UTC') STEP INTERVAL 1 YEAR);
SELECT count() FROM (SELECT toDateTime64('9999-06-01 00:00:00', 0, 'UTC') AS t ORDER BY t DESC WITH FILL TO toDateTime64('9999-01-01 00:00:00', 0, 'UTC') STEP INTERVAL -1 MONTH);

SELECT 'an INTERVAL step that would leave the column type before an in-range TO stops at TO';

-- The calendar arithmetic of an INTERVAL step over Date and DateTime used to wrap around the storage type of the
-- column, and the wrapped-around sequence kept cycling below TO. A step past the boundary of the type is past
-- any TO within it, so the fill stops there - even for a step longer than the whole span of the type, which used
-- to wrap forward.
SELECT groupArray(t) FROM (SELECT toDateTime('2106-01-01 00:00:00', 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime('2106-01-01 00:00:00', 'UTC') TO 4294967295 STEP INTERVAL 100 YEAR);
SELECT groupArray(t) FROM (SELECT toDateTime('2000-01-01 00:00:00', 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime('2000-01-01 00:00:00', 'UTC') TO 4294967295 STEP INTERVAL 50 YEAR);
SELECT groupArray(t) FROM (SELECT toDateTime('2100-01-01 00:00:00', 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime('2100-01-01 00:00:00', 'UTC') TO toDateTime('2106-01-01 00:00:00', 'UTC') STEP INTERVAL 10 YEAR);
SELECT groupArray(t) FROM (SELECT toDateTime('1970-01-01 00:00:00', 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime('1970-01-01 00:00:00', 'UTC') TO toDateTime('2106-01-01 00:00:00', 'UTC') STEP INTERVAL 150 YEAR);
SELECT groupArray(t) FROM (SELECT toDateTime('1970-01-01 00:00:00', 'UTC') AS t ORDER BY t ASC WITH FILL TO toDateTime('2106-01-01 00:00:00', 'UTC') STEP INTERVAL 150 YEAR);
SELECT groupArray(d) FROM (SELECT toDate('2148-01-01') AS d ORDER BY d ASC WITH FILL FROM toDate('2148-01-01') TO 65535 STEP INTERVAL 5 YEAR);
SELECT count(), min(t), max(t) FROM (SELECT toDateTime('2106-01-01 00:00:00', 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime('2106-01-01 00:00:00', 'UTC') TO toDateTime('2106-01-05 00:00:00', 'UTC') STEP INTERVAL 1 DAY);
-- The length in seconds of a long HOUR / MINUTE / SECOND step used to wrap around Int64 (5124095576030432 hours
-- wrap to 3584 seconds), so the fill generated small values ahead of the anchor instead of stopping.
SELECT groupArray(t) FROM (SELECT toDateTime(0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM t TO 10000 STEP INTERVAL 5124095576030432 HOUR);
SELECT groupArray(t) FROM (SELECT toDateTime(0, 'UTC') AS t ORDER BY t ASC WITH FILL TO 10000 STEP INTERVAL 307445734561825861 MINUTE);
SELECT groupArray(t) FROM (SELECT toDateTime(10000, 'UTC') AS t ORDER BY t DESC WITH FILL TO 0 STEP INTERVAL -5124095576030432 HOUR);

SELECT 'a numeric step that overflows the Int64 arithmetic carrier stops at TO';

-- The arithmetic for all integer fill columns uses an Int64 carrier. The step below overflows it after
-- INT64_MAX - 1: the value it overflowed from is past TO, so the fill stops there.
SELECT groupArray(x) FROM (SELECT toInt64(9223372036854775806) AS x ORDER BY x ASC WITH FILL FROM x TO toInt64(9223372036854775807) STEP 2);
SELECT groupArray(x) FROM (SELECT toInt64(-9223372036854775807) AS x ORDER BY x DESC WITH FILL FROM x TO toInt64(-9223372036854775808) STEP -2);
SELECT groupArray(x) FROM (SELECT toInt64(9223372036854775800) AS x ORDER BY x ASC WITH FILL TO 9223372036854775807 STEP 5);

-- With no TO, the next original row is the exclusive bound: the first step from INT64_MAX - 1 overflows past it.
SELECT groupArray(x) FROM (SELECT toInt64(9223372036854775807) AS x ORDER BY x ASC WITH FILL FROM toInt64(9223372036854775806) STEP 2);

SELECT 'a generated value out of range of the column type is rejected at runtime';

-- Without FROM the sequence is anchored at a data value, so whether it ever generates a value the column cannot
-- hold is known only at execution time: `TO 257 STEP 3` over a UInt8 column stops at 254 when the data ends at 11,
-- but reaches 256 - which the column stores as 0 - when it ends at 13.
SELECT x FROM (SELECT toUInt8(13) AS x ORDER BY x ASC WITH FILL TO 257 STEP 3) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT groupArray(x) FROM (SELECT toUInt8(11) AS x ORDER BY x ASC WITH FILL TO 257 STEP 3);
-- The same for a signed column type.
SELECT x FROM (SELECT toInt8(126) AS x ORDER BY x ASC WITH FILL TO 130 STEP 3) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT groupArray(x) FROM (SELECT toInt8(127) AS x ORDER BY x ASC WITH FILL TO 130 STEP 3);

SELECT 'a fill towards a STALENESS border out of range of the column type is rejected';

-- STALENESS moves the effective bound to a fixed distance past the last data row, so the bound is known only at
-- execution time as well. The border itself is not validated - the fill is rejected by the same
-- per-generated-value check as everywhere else, once it reaches a value out of range of the column type -
-- including the calendar window of Date32 and DateTime64, where the values between the calendar and the storage
-- boundary all serialize as the clamped boundary date (without the check the first query below returns
-- 9999-12-31 23:59:59 nine times in a row).
SELECT t FROM (SELECT toDateTime64('9999-12-31 23:59:58', 0, 'UTC') AS t ORDER BY t ASC WITH FILL STALENESS 10) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT d FROM (SELECT toDate32(2932896) AS d ORDER BY d ASC WITH FILL STALENESS 10) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT x FROM (SELECT toUInt8(250) AS x ORDER BY x ASC WITH FILL STALENESS 20) FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- A border that lands exactly on the last representable time point is fine.
SELECT count(), max(t) FROM (SELECT toDateTime64('9999-12-31 23:59:49', 0, 'UTC') AS t ORDER BY t ASC WITH FILL STALENESS 10);
SELECT count(), max(t) FROM (SELECT toDateTime64('2020-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t ASC WITH FILL STALENESS 10);

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

SELECT 'NaN and infinity in a float fill column stay accepted';

-- A step cannot advance NaN or infinity, and such values simply stop the filling at the border, as they always
-- did, instead of rejecting the query: the step-advance check fires only for a sequence that would otherwise
-- never terminate.
SELECT a, b FROM (SELECT 1 AS a, nan AS b UNION ALL SELECT 3, 100.) ORDER BY a WITH FILL, b WITH FILL TO 3 STEP 1;
SELECT a, b FROM (SELECT 1 AS a, inf AS b UNION ALL SELECT 3, 100.) ORDER BY a WITH FILL, b WITH FILL TO 3 STEP 1;
SELECT b FROM (SELECT 0. AS b UNION ALL SELECT nan UNION ALL SELECT 5.) ORDER BY b WITH FILL STALENESS 2;
-- A staleness border the step arithmetic fails to advance (1e300 + 1 == 1e300 in the float precision) simply
-- generates nothing, as on any stagnated border, instead of rejecting the query.
SELECT x FROM (SELECT 1e300 AS x) ORDER BY x WITH FILL STALENESS 1;
-- A NaN literal TO is rejected up front.
SELECT b FROM (SELECT 5. AS b) ORDER BY b WITH FILL TO nan STEP 1; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'the runtime check covers a trailing fill column as well';

-- While the fill of a leading column advances, a trailing fill column is refilled towards its own TO from an
-- anchor taken from the data, so the per-generated-value check must fire in that branch too: anchored at 13,
-- the trailing fill below reaches 256 (which the column would store as 0) before stopping under TO 257.
SELECT a, b FROM (SELECT 1 AS a, toUInt8(13) AS b UNION ALL SELECT 3, toUInt8(20)) ORDER BY a WITH FILL, b WITH FILL TO 257 STEP 3 FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }
-- Anchored at 11 it stops at 254 and stays an ordinary fill.
SELECT count(), max(b) FROM (SELECT a, b FROM (SELECT 1 AS a, toUInt8(11) AS b UNION ALL SELECT 3, toUInt8(20)) ORDER BY a WITH FILL, b WITH FILL TO 257 STEP 3);
-- The walk of a trailing fill column towards a TO past the range of the column type is rejected when the walk
-- starts, not after generating every value up to the boundary of the type (2^32 rows for `DateTime`).
SELECT a, t FROM (SELECT 1 AS a, toDateTime(5, 'UTC') AS t UNION ALL SELECT 2 AS a, toDateTime(5, 'UTC') AS t) ORDER BY a ASC WITH FILL, t ASC WITH FILL TO 4294967297 STEP 1 FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'a fixed-length INTERVAL step over DateTime64 saturates at the boundary of the storage type';

-- A step of seconds or a finer kind used to throw `DECIMAL_OVERFLOW` once the ticks left Int64, also in the long
-- jumps of a fill with `STALENESS`, and a step of minutes or hours wrapped around and never terminated.
SELECT t FROM (SELECT toDateTime64('2262-04-11 23:47:14', 9, 'UTC') AS t) ORDER BY t WITH FILL FROM toDateTime64('2262-04-11 23:47:14', 9, 'UTC') TO toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC') STEP INTERVAL 1 SECOND;
SELECT t FROM (SELECT toDateTime64('2262-04-11 23:47:14', 9, 'UTC') AS t) ORDER BY t WITH FILL TO toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC') STEP INTERVAL 1 MINUTE;
SELECT t FROM (SELECT toDateTime64('1700-01-01 00:00:00', 9, 'UTC') AS t UNION ALL SELECT toDateTime64('2262-04-11 23:47:14', 9, 'UTC')) ORDER BY t WITH FILL STEP INTERVAL 1 SECOND STALENESS INTERVAL 2 SECOND;

SELECT 'an INTERVAL step finer than the scale of DateTime64 is converted to whole ticks of the column';

-- `toDateTime64(..., 2) + INTERVAL 10 MILLISECOND` is a `DateTime64(3)`, so the step used to store a value of the
-- finer scale back into the column of scale 2, ten times further in time.
SELECT t FROM (SELECT toDateTime64('2000-01-01 00:00:00', 2, 'UTC') AS t) ORDER BY t WITH FILL TO toDateTime64('2000-01-01 00:00:00.05', 2, 'UTC') STEP INTERVAL 10 MILLISECOND;
SELECT t FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'UTC') AS t) ORDER BY t WITH FILL TO toDateTime64('2000-01-01 00:00:03', 0, 'UTC') STEP INTERVAL 1000000 MICROSECOND;
-- A step that is not a whole number of ticks of the column is rejected.
SELECT t FROM (SELECT toDateTime64('2000-01-01 00:00:00', 2, 'UTC') AS t) ORDER BY t WITH FILL TO toDateTime64('2000-01-01 00:00:01', 2, 'UTC') STEP INTERVAL 15 MILLISECOND; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT t FROM (SELECT toDateTime64('2000-01-01 00:00:00', 0, 'UTC') AS t) ORDER BY t WITH FILL TO toDateTime64('2000-01-01 00:00:01', 0, 'UTC') STEP INTERVAL 1 NANOSECOND; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'a step whose ticks overflow Int64 but whose result fits is not saturated';

-- 200000000 minutes are 1.2 * 10^19 ticks of `DateTime64(9)`, which do not fit Int64, but added to the negative
-- anchor they land in 2080, which the column holds.
SELECT t FROM (SELECT toDateTime64('1700-01-01 00:00:00', 9, 'UTC') AS t) ORDER BY t WITH FILL TO toDateTime64('2200-01-01 00:00:00', 9, 'UTC') STEP INTERVAL 200000000 MINUTE;
SELECT t FROM (SELECT toDateTime64('2200-01-01 00:00:00', 9, 'UTC') AS t) ORDER BY t DESC WITH FILL TO toDateTime64('1700-01-01 00:00:00', 9, 'UTC') STEP INTERVAL -200000000 MINUTE;
-- The same interval as `STALENESS` bounds the window at the same value.
SELECT t FROM (SELECT toDateTime64('1700-01-01 00:00:00', 9, 'UTC') AS t UNION ALL SELECT toDateTime64('1800-01-01 00:00:00', 9, 'UTC')) ORDER BY t WITH FILL STEP INTERVAL 100000000 MINUTE STALENESS INTERVAL 200000000 MINUTE;
-- A `STALENESS` window past the range of the column is not cut at the boundary of the storage: the filling of the
-- window after 2200 would continue the steps from 1700 to 2270, which the column cannot hold.
SELECT t FROM (SELECT toDateTime64('1700-01-01 00:00:00', 9, 'UTC') AS t UNION ALL SELECT toDateTime64('2200-01-01 00:00:00', 9, 'UTC')) ORDER BY t WITH FILL STEP INTERVAL 100000000 MINUTE STALENESS INTERVAL 200000000 MINUTE FORMAT Null; -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'a calendar step does not wrap around before it is compared';

-- 650 years after 1700 is 2350, past `TO` and past the range of `DateTime64(9)`: the filling stops at the anchor.
SELECT t FROM (SELECT toDateTime64('1700-01-01 00:00:00', 9, 'UTC') AS t) ORDER BY t WITH FILL TO toDateTime64('2200-01-01 00:00:00', 9, 'UTC') STEP INTERVAL 650 YEAR;
-- A day step that does not fit `Int32` used to wrap to a single day.
SELECT count() FROM (SELECT toDate32('1970-01-01') AS d ORDER BY d WITH FILL TO 3 STEP INTERVAL 4294967297 DAY);
SELECT count() FROM (SELECT toDate('1970-01-01') AS d ORDER BY d WITH FILL TO 3 STEP INTERVAL 4294967297 DAY);

SELECT 'a numeric step truncated to zero ticks does not advance';

SELECT t FROM (SELECT toDateTime64('9999-12-31 23:59:58', 2, 'UTC') AS t) ORDER BY t WITH FILL FROM toDateTime64('9999-12-31 23:59:58', 2, 'UTC') TO toDateTime64('9999-12-31 23:59:58', 2, 'UTC') + INTERVAL 10 SECOND STEP 0.001;

SELECT 'a quarter step whose number of months overflows Int64 does not wrap around';

-- `3 * 6148914691236517206` months wrap to 2, which used to generate a row every two months.
SELECT groupArray(t) FROM (SELECT toDateTime('2000-01-01 00:00:00', 'UTC') AS t ORDER BY t WITH FILL TO toDateTime('2001-01-01 00:00:00', 'UTC') STEP INTERVAL 6148914691236517206 QUARTER);
SELECT groupArray(t) FROM (SELECT toDateTime('2001-01-01 00:00:00', 'UTC') AS t ORDER BY t DESC WITH FILL TO toDateTime('2000-01-01 00:00:00', 'UTC') STEP INTERVAL -6148914691236517206 QUARTER);

SELECT 'a bound past the storage of DateTime64 is not confused with its last value';

-- At scale 9 the maximum of Int64 is a valid value of the column, so neither the exclusive `STALENESS` bound nor a
-- step past it may be saturated to it.
SELECT groupArray(toUnixTimestamp64Nano(t)) FROM (SELECT toDateTime64('2262-04-11 23:47:16.854775805', 9, 'UTC') AS t ORDER BY t WITH FILL STEP INTERVAL 1 NANOSECOND STALENESS INTERVAL 3 NANOSECOND);
SELECT groupArray(toUnixTimestamp64Nano(t)) FROM (SELECT toDateTime64('2262-04-11 23:47:16.854775805', 9, 'UTC') AS t ORDER BY t WITH FILL STEP INTERVAL 2 NANOSECOND STALENESS INTERVAL 4 NANOSECOND);
-- A `STALENESS` bound past the storage lets the filling run up to its last value, and the next one is rejected as a
-- value the column cannot hold, as for any other column type.
SELECT groupArray(toUnixTimestamp64Nano(t)) FROM (SELECT toDateTime64('2262-04-11 23:47:16.854775805', 9, 'UTC') AS t ORDER BY t WITH FILL STEP INTERVAL 1 NANOSECOND STALENESS INTERVAL 1 DAY); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT groupArray(toUnixTimestamp64Nano(t)) FROM (SELECT toDateTime64('1900-01-01 00:00:00.000000002', 9, 'UTC') AS t ORDER BY t DESC WITH FILL STEP INTERVAL -1 NANOSECOND STALENESS INTERVAL -3 NANOSECOND);

SELECT 'a numeric STALENESS bound past the storage does not wrap around behind the anchor';

-- The numeric step wraps around in the `Int64`/`UInt64` carrier or in the `Decimal64` ticks of `DateTime64`, so a
-- `STALENESS` border past the end of the carrier used to land behind the anchor and stop the filling right there.
SELECT groupArray(x) FROM (SELECT toInt64(9223372036854775805) AS x ORDER BY x WITH FILL TO toInt64(9223372036854775807) STALENESS 3);
SELECT groupArray(x) FROM (SELECT toInt64(9223372036854775805) AS x ORDER BY x WITH FILL STALENESS 3);
SELECT groupArray(x) FROM (SELECT toInt64(-9223372036854775806) AS x ORDER BY x DESC WITH FILL TO toInt64(-9223372036854775808) STALENESS -3);
SELECT groupArray(x) FROM (SELECT toInt64(-9223372036854775806) AS x ORDER BY x DESC WITH FILL STALENESS -3);
SELECT groupArray(x) FROM (SELECT toUInt64(18446744073709551613) AS x ORDER BY x WITH FILL STALENESS 3);
SELECT groupArray(x) FROM (SELECT arrayJoin([toInt64(9223372036854775800), toInt64(9223372036854775805)]) AS x ORDER BY x WITH FILL STALENESS 3);
SELECT groupArray(toUnixTimestamp64Nano(t)) FROM (SELECT toDateTime64('2262-04-11 23:47:16.854775805', 9, 'UTC') AS t ORDER BY t WITH FILL TO toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC') STEP 0.000000001 STALENESS 0.000000003);
SELECT groupArray(toUnixTimestamp64Nano(t)) FROM (SELECT toDateTime64('2262-04-11 23:47:16.854775805', 9, 'UTC') AS t ORDER BY t WITH FILL STEP 0.000000001 STALENESS 0.000000003);
SELECT groupArray(toUnixTimestamp64Nano(t)) FROM (SELECT fromUnixTimestamp64Nano(toInt64(-9223372036854775806), 'UTC') AS t ORDER BY t DESC WITH FILL TO fromUnixTimestamp64Nano(toInt64(-9223372036854775808), 'UTC') STEP -0.000000001 STALENESS -0.000000003);
SELECT groupArray(toUnixTimestamp64Nano(t)) FROM (SELECT fromUnixTimestamp64Nano(toInt64(-9223372036854775806), 'UTC') AS t ORDER BY t DESC WITH FILL STEP -0.000000001 STALENESS -0.000000003);
-- An INTERVAL step over Date and DateTime continues past the boundary of the storage type in the wider calendar,
-- so a TO out of range is rejected only when the first step past the boundary is still before it: otherwise the
-- filling ends at TO without generating a value the column cannot hold.
SELECT groupArray(d) FROM (SELECT toDate('2149-06-01') AS d ORDER BY d ASC WITH FILL FROM toDate('2149-06-01') TO 65540 STEP INTERVAL 1 MONTH);
SELECT groupArray(d) FROM (SELECT toDate('2149-06-01') AS d ORDER BY d ASC WITH FILL FROM toDate('2149-06-01') TO 65600 STEP INTERVAL 1 MONTH); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT groupArray(d) FROM (SELECT toDate('2149-05-30') AS d ORDER BY d ASC WITH FILL FROM toDate('2149-05-30') TO 65540 STEP INTERVAL 1 WEEK);
SELECT groupArray(d) FROM (SELECT toDate('2149-05-30') AS d ORDER BY d ASC WITH FILL FROM toDate('2149-05-30') TO 65545 STEP INTERVAL 1 WEEK); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count(), min(d), max(d) FROM (SELECT toDate(0) AS d ORDER BY d ASC WITH FILL FROM toDate(0) TO 65537 STEP INTERVAL 2 DAY); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count(), min(d), max(d) FROM (SELECT toDate(1) AS d ORDER BY d ASC WITH FILL FROM toDate(1) TO 65537 STEP INTERVAL 2 DAY);
SELECT groupArray(t) FROM (SELECT toDateTime('2106-01-01 00:00:00', 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime('2106-01-01 00:00:00', 'UTC') TO 4294967297 STEP INTERVAL 1 YEAR);
SELECT groupArray(t) FROM (SELECT toDateTime('2106-01-01 00:00:00', 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime('2106-01-01 00:00:00', 'UTC') TO 4300000000 STEP INTERVAL 1 MONTH); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT groupArray(toUnixTimestamp(t)) FROM (SELECT toDateTime(4294967290, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime(4294967290, 'UTC') TO 4294967300 STEP INTERVAL 10 SECOND);
SELECT groupArray(toUnixTimestamp(t)) FROM (SELECT toDateTime(4294967290, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime(4294967290, 'UTC') TO 4294967300 STEP INTERVAL 3 SECOND); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT count() FROM (SELECT toDateTime(0, 'UTC') AS t ORDER BY t ASC WITH FILL FROM toDateTime(0, 'UTC') TO 4294967297 STEP INTERVAL 1 SECOND); -- { serverError INVALID_WITH_FILL_EXPRESSION }

SELECT 'a UInt64 fill from an explicit FROM continues past INT64_MAX';

-- FROM and TO of a UInt64 column are held in the Int64 carrier, so the step past `INT64_MAX` used to be taken for an
-- overflow and stop the filling there, while the same sequence anchored at a data value was filled completely.
SELECT groupArray(x) FROM (SELECT toUInt64(9223372036854775810) AS x ORDER BY x WITH FILL FROM 9223372036854775806 STEP 1);
SELECT groupArray(x) FROM (SELECT toUInt64(9223372036854775810) AS x ORDER BY x WITH FILL FROM 9223372036854775806 STEP 2);
SELECT groupArray(x) FROM (SELECT arrayJoin([toUInt64(9223372036854775806), toUInt64(9223372036854775810)]) AS x ORDER BY x WITH FILL STEP 1);
SELECT groupArray(x) FROM (SELECT toUInt64(18446744073709551614) AS x ORDER BY x WITH FILL FROM 9223372036854775806 STEP 4611686018427387904);
SELECT groupArray(x) FROM (SELECT toUInt64(9223372036854775810) AS x ORDER BY x DESC WITH FILL TO 9223372036854775805 STEP -1);

-- A STALENESS step that is not a whole number of ticks of a DateTime64 column is never materialized: it is an
-- exclusive bound, which is rounded away from zero to whole ticks.
SELECT groupArray(t) FROM (SELECT toDateTime64('2020-01-01 00:00:00', 0, 'UTC') AS t ORDER BY t WITH FILL TO toDateTime64('2020-01-01 00:00:01', 0, 'UTC') STALENESS INTERVAL 1 MILLISECOND);
SELECT groupArray(t) FROM (SELECT toDateTime64('2020-01-01 00:00:00', 1, 'UTC') AS t ORDER BY t WITH FILL TO toDateTime64('2020-01-01 00:00:01', 1, 'UTC') STEP INTERVAL 100 MILLISECOND STALENESS INTERVAL 250 MILLISECOND);
SELECT groupArray(t) FROM (SELECT toDateTime64('2020-01-01 00:00:01', 1, 'UTC') AS t ORDER BY t DESC WITH FILL TO toDateTime64('2020-01-01 00:00:00', 1, 'UTC') STEP INTERVAL -100 MILLISECOND STALENESS INTERVAL -250 MILLISECOND);
