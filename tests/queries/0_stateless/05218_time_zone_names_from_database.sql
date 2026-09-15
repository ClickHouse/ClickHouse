-- A time zone name is accepted only if it is in the time zone database linked into the binary, which
-- is exactly what `system.time_zones` lists, or if it is a fixed offset of `05098`. `cctz` on its own
-- also resolves a name as a path under the time zone database directory, which accepts unboundedly
-- many spellings of the same zone, and, with a `file:` prefix, any absolute path at all. Each distinct
-- spelling that got loaded was a permanent ~4.6 MiB entry in the `DateLUT` cache, which never evicts
-- anything, so untrusted input could make the server allocate memory that it never gives back.

-- Names from the time zone database.
SELECT toDateTime(0, 'UTC'), toDateTime(0, 'Europe/Amsterdam'), toDateTime64(0, 3, 'Asia/Kathmandu');
SELECT timeZone() SETTINGS session_timezone = 'Europe/Amsterdam';
SELECT count() > 100 FROM system.time_zones;

-- Dot segments and repeated separators inside a name from the database: a different name for the same
-- zone, and any number of them.
SELECT toDateTime(0, 'Europe/./Amsterdam'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'Europe/././Amsterdam'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'Europe//Amsterdam'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime64(0, 3, './Europe/Amsterdam'); -- { serverError BAD_ARGUMENTS }
SELECT CAST(0 AS DateTime('Europe/Amsterdam/')); -- { serverError BAD_ARGUMENTS }

-- The `file:` prefix takes the rest of the name as a path, so it both spells one zone in unboundedly
-- many ways and reads a file outside the time zone database directory.
SELECT toDateTime(0, 'file:UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'file:/usr/share/zoneinfo/UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'file:/etc/passwd'); -- { serverError BAD_ARGUMENTS }

-- A path instead of a name.
SELECT toDateTime(0, '/usr/share/zoneinfo/UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, '../usr/share/zoneinfo/UTC'); -- { serverError BAD_ARGUMENTS }

-- The database is case-sensitive; a case-insensitive filesystem used to accept every mixture.
SELECT toDateTime(0, 'europe/amsterdam'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'EUROPE/AMSTERDAM'); -- { serverError BAD_ARGUMENTS }

-- A misspelled name is reported instead of being resolved through the host.
SELECT toDateTime(0, 'Europe/Amsterdm'); -- { serverError BAD_ARGUMENTS }

-- The setting is validated where it is set, so it cannot be accepted and then fail on every query.
SET session_timezone = 'Europe/./Amsterdam'; -- { serverError BAD_ARGUMENTS }
SET session_timezone = 'file:/usr/share/zoneinfo/UTC'; -- { serverError BAD_ARGUMENTS }
