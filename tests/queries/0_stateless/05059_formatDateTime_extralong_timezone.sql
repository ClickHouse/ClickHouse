-- `formatDateTimeInJodaSyntax` reserves `MAX_JODA_TIMEZONE_NAME_LENGTH` (32) bytes per row for the
-- `zzzz` formatter and checks the length of the name at run time, so the longest name in the time
-- zone database is what the reservation has to fit exactly. That is
-- `America/Argentina/ComodRivadavia`, at 32 bytes:
-- `SELECT max(length(time_zone)) FROM system.time_zones` is 32.
-- A longer name can no longer be constructed: padding a real name with extra separators used to be
-- accepted, and is not, see `05218_time_zone_names_from_database`.

SELECT length(formatDateTimeInJodaSyntax(toDateTime(0), 'zzzz', 'Etc/UTC'));
SELECT formatDateTimeInJodaSyntax(toDateTime(0), 'zzzz', 'America/Argentina/ComodRivadavia');
SELECT length(formatDateTimeInJodaSyntax(toDateTime(0), 'zzzz', 'America/Argentina/ComodRivadavia'));
SELECT max(length(time_zone)) = 32 FROM system.time_zones;
