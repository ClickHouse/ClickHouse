-- Exercise the many MySQL-format and Joda-format specifiers in
-- Functions/parseDateTime.cpp (mysql* / joda* methods inside the Instruction
-- struct and the parseMysqlFormat/parseJodaFormat switches in the factory).

-- CI randomizes the session timezone; pin it so zero/default DateTime values
-- render consistently.
SET session_timezone = 'UTC';

SELECT '--- mysqlDayOfWeekTextShort: %a ---';
SELECT parseDateTime('Mon', '%a');
SELECT parseDateTime('TUE', '%a');
SELECT parseDateTime('wed', '%a');

SELECT '--- mysqlDayOfWeekTextShort: unknown text ---';
SELECT parseDateTime('XyZ', '%a'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime('M', '%a'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT '--- mysqlDayOfWeekTextLong: %W ---';
SELECT parseDateTime('Monday', '%W');
SELECT parseDateTime('TUESDAY', '%W');
SELECT parseDateTime('wednesday', '%W');
SELECT parseDateTime('Sunday', '%W');

SELECT '--- mysqlDayOfWeekTextLong: unknown 3-letter prefix ---';
SELECT parseDateTime('XyZrest', '%W'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime('Monxxx', '%W'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime('Mon', '%W'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT '--- mysqlMonthOfYearTextShort: %b ---';
SELECT parseDateTime('Jan', '%b');
SELECT parseDateTime('FEB', '%b');
SELECT parseDateTime('dec', '%b');

SELECT '--- mysqlMonthOfYearTextShort: unknown ---';
SELECT parseDateTime('Xyz', '%b'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT '--- mysqlMonthOfYearTextLong: %M ---';
SELECT parseDateTime('January', '%M');
SELECT parseDateTime('FEBRUARY', '%M');
SELECT parseDateTime('september', '%M');

SELECT '--- mysqlMonthOfYearTextLong: unknown ---';
SELECT parseDateTime('Junxxx', '%M'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime('Jul', '%M'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime('XyzExtra', '%M'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT '--- mysqlDayOfYear: %j ---';
SELECT parseDateTime('001', '%j');
SELECT parseDateTime('100', '%j');
SELECT parseDateTime('365', '%j');

SELECT '--- mysqlDayOfWeek: %w ---';
SELECT parseDateTime('3', '%w');
SELECT parseDateTime('7', '%w');

SELECT '--- mysqlISO8601Year4: %Y (extra edge: setYear boundaries) ---';
SELECT parseDateTime('1970', '%Y', 'UTC');
SELECT parseDateTime('2106', '%Y', 'UTC');
SELECT parseDateTime('1969', '%Y', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime('2107', '%Y', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT '--- mysqlYear2: %y ---';
SELECT parseDateTime('99', '%y', 'UTC');
SELECT parseDateTime('00', '%y', 'UTC');
SELECT parseDateTime('70', '%y', 'UTC');

SELECT '--- mysqlISO8601Week: %V ---';
SELECT parseDateTime('05', '%V');

SELECT '--- Unsupported MySQL specifiers ---';
SELECT parseDateTime('2023', '%U'); -- { serverError NOT_IMPLEMENTED }
SELECT parseDateTime('2023', '%v'); -- { serverError NOT_IMPLEMENTED }
SELECT parseDateTime('2023', '%x'); -- { serverError NOT_IMPLEMENTED }
SELECT parseDateTime('2023', '%X'); -- { serverError NOT_IMPLEMENTED }
SELECT parseDateTime('2023', '%Q'); -- { serverError NOT_IMPLEMENTED }

SELECT '--- Unknown MySQL specifier ---';
SELECT parseDateTime('2023', '%Z'); -- { serverError BAD_ARGUMENTS }

SELECT '--- mysqlAmericanDate: %D (mm/dd/yy) ---';
SELECT parseDateTime('01/02/99', '%D');
SELECT parseDateTime('12/31/00', '%D');
SELECT parseDateTime('1st', '%D'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT '--- 12-hour format: %h / %I + %p ---';
SELECT parseDateTime('11:30:00 PM', '%h:%i:%s %p', 'UTC');
SELECT parseDateTime('11:30:00 AM', '%h:%i:%s %p', 'UTC');
SELECT parseDateTime('12:00:00 AM', '%I:%i:%s %p', 'UTC');
SELECT parseDateTime('12:00:00 PM', '%I:%i:%s %p', 'UTC');

SELECT '--- %s timestamp: out-of-range, malformed ---';
SELECT parseDateTime('1234567890', '%s'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT '--- Error fallback: parseDateTimeOrZero / parseDateTimeOrNull ---';
SELECT parseDateTimeOrZero('bad', '%Y');
SELECT parseDateTimeOrNull('bad', '%Y');
SELECT parseDateTimeOrZero('Xyz', '%a');
SELECT parseDateTimeOrNull('Xyz', '%a');

SELECT '--- Timezone argument ---';
SELECT parseDateTime('2023-01-02 03:04:05', '%Y-%m-%d %H:%i:%s', 'UTC');
SELECT parseDateTime('2023-01-02 03:04:05', '%Y-%m-%d %H:%i:%s', 'Asia/Tokyo');

SELECT '--- Literal characters in format ---';
SELECT parseDateTime('T2023-01-02Z', 'T%Y-%m-%dZ');

SELECT '--- Joda literals: escaping and missing quotes ---';
SELECT parseDateTimeInJodaSyntax('2023 AD', 'yyyy'' ''G');
SELECT parseDateTimeInJodaSyntax('2023', 'yyyy'''); -- { serverError BAD_ARGUMENTS }

SELECT '--- Joda fields ---';
SELECT parseDateTimeInJodaSyntax('2023-01-02', 'yyyy-MM-dd');
SELECT parseDateTimeInJodaSyntax('2023-01-02 03:04:05', 'yyyy-MM-dd HH:mm:ss');
SELECT parseDateTimeInJodaSyntax('0023', 'yyyy'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT '--- parseDateTime64 variants ---';
SELECT parseDateTime64('2023-01-02 03:04:05.123456', '%Y-%m-%d %H:%i:%s.%f');
SELECT parseDateTime64OrZero('bad', '%Y');
SELECT parseDateTime64OrNull('bad', '%Y');
SELECT parseDateTime64InJodaSyntax('2023-01-02 03:04:05.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS');
SELECT parseDateTime64InJodaSyntaxOrZero('bad', 'yyyy');
SELECT parseDateTime64InJodaSyntaxOrNull('bad', 'yyyy');

SELECT '--- parseDateTime64 wrong type argument ---';
SELECT parseDateTime64('2023', '%Y', 6); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
