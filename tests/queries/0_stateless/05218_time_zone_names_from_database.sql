-- A time zone name is accepted only if it is in the time zone database linked into the binary, which
-- is exactly what `system.time_zones` lists, or if it is a fixed offset of `05098`. `cctz` on its own
-- also resolves a name as a path under the time zone database directory, which accepts unboundedly
-- many spellings of the same zone, and, with a `file:` prefix, any absolute path at all; and the
-- lookup of the built-in database used to take a C string, so a name with an embedded `\0` matched the
-- zone that its prefix names. Each distinct spelling that got loaded was a permanent ~4.6 MiB entry in
-- the `DateLUT` cache, which never evicts anything, so untrusted input could make the server allocate
-- memory that it never gives back.

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

-- A name is matched by length, not as a C string. Names arrive from length-prefixed carriers (a
-- literal, `readStringBinary` in binary type decoding), so a name can carry an embedded `\0` while
-- the cache keys on the whole byte string: matching the prefix would load one zone under unboundedly
-- many keys.
SELECT toDateTime(0, 'UTC\0'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'UTC\0suffix'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime64(0, 3, 'Europe/Amsterdam\0suffix'); -- { serverError BAD_ARGUMENTS }
SELECT CAST(0 AS DateTime('Fixed/UTC+05:30:00\0suffix')); -- { serverError BAD_ARGUMENTS }

-- The setting is validated where it is set, so it cannot be accepted and then fail on every query.
SET session_timezone = 'Europe/./Amsterdam'; -- { serverError BAD_ARGUMENTS }
SET session_timezone = 'file:/usr/share/zoneinfo/UTC'; -- { serverError BAD_ARGUMENTS }
SET session_timezone = 'UTC\0suffix'; -- { serverError BAD_ARGUMENTS }

-- A fixed offset has exactly one spelling. `cctz` normalizes the components, so an out-of-range
-- minute or second names a zone that a canonical spelling already names - `Fixed/UTC+00:75:00` and
-- `Fixed/UTC+01:14:60` both load the `+01:15` zone - which would give one offset several names, and
-- with them several entries of the cache.
SELECT toDateTime(0, 'Fixed/UTC+01:15:00');
SELECT toDateTime(0, 'Fixed/UTC+00:75:00'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'Fixed/UTC+01:14:60'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'Fixed/UTC-00:75:00'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime64(0, 3, 'Fixed/UTC+00:00:900'); -- { serverError BAD_ARGUMENTS }
SELECT CAST(0 AS DateTime('Fixed/UTC+13:74:60')); -- { serverError BAD_ARGUMENTS }
SET session_timezone = 'Fixed/UTC+00:75:00'; -- { serverError BAD_ARGUMENTS }
