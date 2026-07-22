-- Tags: no-parallel
-- Creates a user with a server-global name, so it cannot run in parallel with itself.

-- `system.users.valid_until` is a `DateTime64`, so it reports the same deadline the
-- authentication check enforces even beyond the `DateTime` (year 2106) ceiling,
-- instead of clamping it.

DROP USER IF EXISTS user_04613_valid_until;

CREATE USER user_04613_valid_until IDENTIFIED WITH no_password VALID UNTIL '9999-12-31 09:59:59 UTC';
SELECT toTypeName(valid_until), toYear(valid_until[1]), toUnixTimestamp64Second(valid_until[1]) FROM system.users WHERE name = 'user_04613_valid_until';

-- A huge interval saturates at the same bound and is reported exactly.
ALTER USER user_04613_valid_until VALID FOR INTERVAL 1000000 YEAR;
SELECT toYear(valid_until[1]), toUnixTimestamp64Second(valid_until[1]) FROM system.users WHERE name = 'user_04613_valid_until';

-- "No expiration" is still reported as zero.
ALTER USER user_04613_valid_until VALID UNTIL 'infinity';
SELECT toUnixTimestamp64Second(valid_until[1]) FROM system.users WHERE name = 'user_04613_valid_until';

DROP USER user_04613_valid_until;
