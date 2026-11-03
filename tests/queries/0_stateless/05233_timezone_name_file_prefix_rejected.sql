-- Time zone names are untrusted input. `cctz` strips the `file:` prefix before it builds the path,
-- so `file:/abs/path` used to bypass the check that rejects names starting with `/` and opened an
-- arbitrary file. See https://github.com/ClickHouse/ClickHouse/issues/121367.

SELECT toDateTime(0, 'file:UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'file:/usr/share/zoneinfo/UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'file:../zoneinfo/UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, '/usr/share/zoneinfo/UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, './UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, '~/UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, '../zoneinfo/UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'Etc/../Etc/UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toTimeZone(toDateTime(0), 'file:UTC'); -- { serverError BAD_ARGUMENTS }
SELECT CAST(0 AS DateTime('file:UTC')); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'UTC') SETTINGS session_timezone = 'file:UTC'; -- { clientError BAD_ARGUMENTS }

-- A regular name still works.
SELECT toDateTime(0, 'UTC');
