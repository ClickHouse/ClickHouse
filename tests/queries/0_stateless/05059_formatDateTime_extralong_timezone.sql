-- Checks that formatDateTimeInJodaSyntax rejects time zone names > 32 characters

SELECT length(formatDateTimeInJodaSyntax(toDateTime(0), 'zzzz', 'Etc/UTC'));
-- backslashes are internally removed
SELECT length(formatDateTimeInJodaSyntax(toDateTime(0), 'zzzz', 'Etc' || repeat('/', 26) || 'UTC'));

-- declined
SELECT length(formatDateTimeInJodaSyntax(toDateTime(0), 'zzzz', 'Etc' || repeat('/', 27) || 'UTC')); -- { serverError BAD_ARGUMENTS }
