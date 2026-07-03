-- Test: exercises post-instruction error paths in `parseDateTime[InJodaSyntax]Or(Null|Zero)`.
-- Covers: src/Functions/parseDateTime.cpp:817-824 ("Invalid format input is malformed at" branch when cur < end)
--         src/Functions/parseDateTime.cpp:828-834 (buildDateTime error path) via the
--         "Seconds since epoch is negative" check at line 632-634.
--
-- Existing tests for *Or(Null|Zero) only exercise instruction-level errors at line 782-810.
-- The branches that fire AFTER the instruction loop (trailing characters, buildDateTime
-- failures) were not covered for the *Or(Null|Zero) variants in any test in 0_stateless or
-- integration.

SELECT 'trailing chars - parseDateTimeOrNull (MySQL)';
SELECT parseDateTimeOrNull('2023-01-02 03:04:05extra', '%Y-%m-%d %H:%i:%s', 'UTC') IS NULL;

SELECT 'trailing chars - parseDateTimeOrZero (MySQL)';
SELECT parseDateTimeOrZero('2023-01-02 03:04:05extra', '%Y-%m-%d %H:%i:%s', 'UTC') = toDateTime('1970-01-01', 'UTC');

SELECT 'trailing chars - parseDateTimeInJodaSyntaxOrNull';
SELECT parseDateTimeInJodaSyntaxOrNull('2023-01-02extra', 'yyyy-MM-dd', 'UTC') IS NULL;

SELECT 'trailing chars - parseDateTimeInJodaSyntaxOrZero';
SELECT parseDateTimeInJodaSyntaxOrZero('2023-01-02extra', 'yyyy-MM-dd', 'UTC') = toDateTime('1970-01-01', 'UTC');

SELECT 'negative seconds_since_epoch - parseDateTimeOrNull (MySQL)';
SELECT parseDateTimeOrNull('1970-01-01 00:00:00', '%Y-%m-%d %H:%i:%s', 'Asia/Shanghai') IS NULL;

SELECT 'negative seconds_since_epoch - parseDateTimeOrZero (MySQL)';
SELECT parseDateTimeOrZero('1970-01-01 00:00:00', '%Y-%m-%d %H:%i:%s', 'Asia/Shanghai') = toDateTime('1970-01-01', 'UTC');

SELECT 'negative seconds_since_epoch - parseDateTimeInJodaSyntaxOrNull';
SELECT parseDateTimeInJodaSyntaxOrNull('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss', 'Asia/Shanghai') IS NULL;

SELECT 'negative seconds_since_epoch - parseDateTimeInJodaSyntaxOrZero';
SELECT parseDateTimeInJodaSyntaxOrZero('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss', 'Asia/Shanghai') = toDateTime('1970-01-01', 'UTC');
