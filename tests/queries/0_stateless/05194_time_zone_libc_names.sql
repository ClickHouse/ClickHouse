-- `cctz` resolves every name of the form `libc:<suffix>` through the C library instead of the time zone
-- database. `libc:localtime` and `libc:UTC` are its own internal, test-only interfaces, and any other
-- suffix is accepted just as well and silently behaves as UTC. The suffix is unrestricted, so this
-- family has no bound, and every name that gets loaded permanently costs ~4.6 MiB in the `DateLUT`
-- cache, which never evicts anything. So the whole prefix is rejected.

SELECT toDateTime(0, 'libc:UTC'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'libc:localtime'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'libc:'); -- { serverError BAD_ARGUMENTS }

-- Every other suffix used to be accepted too, and answered as if it were UTC instead of reporting the
-- name back.
SELECT toDateTime(0, 'libc:Europe/Amsterdam'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime(0, 'libc:/etc/passwd'); -- { serverError BAD_ARGUMENTS }
SELECT toDateTime64(0, 3, 'libc:1'); -- { serverError BAD_ARGUMENTS }
SELECT CAST(0 AS DateTime('libc:2')); -- { serverError BAD_ARGUMENTS }
SELECT CAST(0 AS DateTime64(3, 'libc:3')); -- { serverError BAD_ARGUMENTS }

-- The setting is validated where it is set, so it cannot be accepted and then fail on every query.
SET session_timezone = 'libc:UTC'; -- { serverError BAD_ARGUMENTS }

-- Names from the time zone database, and the fixed offsets of `05098`, are unaffected.
SELECT toDateTime(0, 'UTC'), toDateTime(0, 'Europe/Amsterdam'), toDateTime(0, 'Fixed/UTC+05:30:00');
