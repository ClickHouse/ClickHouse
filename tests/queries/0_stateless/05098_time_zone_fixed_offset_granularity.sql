-- `cctz` synthesizes a zone for every name of the form `Fixed/UTC±HH:MM:SS`, accepting any offset up
-- to 24 hours, which is 172801 distinct names. Each name that gets loaded permanently costs ~4.6 MiB
-- in the `DateLUT` cache, which never evicts anything, so untrusted input could make the server
-- allocate memory that it never gives back. Only the offsets a time zone can actually have are
-- accepted: a whole number of quarters of an hour, no further from UTC than 14 hours.

-- Offsets that a time zone can have. These are the names that reading an Arrow or ORC timestamp
-- column with a fixed-offset time zone produces, see `03926_arrow_fixed_offset_timezone`.
SELECT toDateTime(0, 'Fixed/UTC+00:00:00'), toDateTime(0, 'Fixed/UTC+05:30:00'), toDateTime(0, 'Fixed/UTC-08:00:00');
SELECT toDateTime64(0, 3, 'Fixed/UTC+14:00:00'), toDateTime64(0, 3, 'Fixed/UTC-14:00:00');
SELECT toString(toDateTime(1700000000, 'Fixed/UTC+05:45:00')) = toString(toDateTime(1700000000, 'Asia/Kathmandu'));
SELECT timeZone() SETTINGS session_timezone = 'Fixed/UTC+01:15:00';

-- Finer than a quarter of an hour.
SELECT toDateTime(0, 'Fixed/UTC+00:07:00'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'Fixed/UTC+05:30:01'); -- { serverError BAD_ARGUMENTS }
SELECT CAST(0 AS DateTime64(3, 'Fixed/UTC-00:44:30')); -- { serverError BAD_ARGUMENTS }

-- Further from UTC than any time zone.
SELECT toDateTime(0, 'Fixed/UTC+14:15:00'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'Fixed/UTC-23:00:00'); -- { serverError BAD_ARGUMENTS }

-- Not a fixed offset at all, so no zone can be synthesized for it.
SELECT toDateTime(0, 'Fixed/UTC+1:00:00'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'Fixed/Europe/Amsterdam'); -- { serverError BAD_ARGUMENTS }

-- The setting is validated where it is set, so it cannot be accepted and then fail on every query.
SET session_timezone = 'Fixed/UTC+00:07:00'; -- { serverError BAD_ARGUMENTS }

-- Names from the time zone database are unaffected.
SELECT count() > 100 FROM system.time_zones;
SELECT toDateTime(0, 'UTC'), toDateTime(0, 'Europe/Amsterdam'), toDateTime64(0, 3, 'Asia/Kathmandu');
