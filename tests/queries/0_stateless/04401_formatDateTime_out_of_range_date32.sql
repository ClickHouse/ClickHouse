-- Formatting an out-of-range Date32 (a day number outside the representable [0000, 9999] window) must saturate
-- the day index to that window, so the day-of-year stays in [1, 366]. Otherwise `formatDateTime` with `%j`
-- reads out of bounds in `writeNumber2` / `writeNumber3` and trips the `v >= 0 && v <= 99` assertion.
-- Caused by the DateLUT extended-range escape paths; found by the AST fuzzer over `00718_format_datetime.sql`.

SELECT toDayOfYear(toDate32(toIntervalSecond(-2147483648), 'UTC'));
SELECT toDayOfYear(toDate32(toIntervalSecond(2147483647), 'UTC'));

SELECT formatDateTime(toDate32(toIntervalSecond(-2147483648), 'UTC'), '%F %j', 'UTC');
SELECT formatDateTime(toDate32(toIntervalSecond(2147483647), 'UTC'), '%F %j', 'UTC');

SELECT formatDateTimeInJodaSyntax(toDate32(toIntervalSecond(-2147483648), 'UTC'), 'yyyy-MM-dd DDD', 'UTC');
SELECT formatDateTimeInJodaSyntax(toDate32(toIntervalSecond(2147483647), 'UTC'), 'yyyy-MM-dd DDD', 'UTC');

-- The exact query the AST fuzzer reduced to (must not abort under a chassert build).
SELECT formatDateTime(toDate32(toIntervalSecond(-2147483648), '2000-12-31'), '%j');
