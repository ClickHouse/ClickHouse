-- Tags: no-parallel
-- ^ creates a globally-named user (the ALTER cases below); the flaky check runs the same test
--   concurrently, so a fixed user name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between
--   parallel repetitions.

-- `parseDateTimeBestEffort` stops at the first thing it cannot interpret instead of requiring the
-- whole `VALID UNTIL` literal to be a datetime. A 20-digit all-digit string used to slip through
-- that way: the first 19 digits were read as a nanosecond-scale Unix timestamp and the last digit
-- was left unread, so an out-of-range deadline such as '18446744327111802015' silently resolved to
-- a live one (fail-open) instead of being rejected. Any literal with left-over characters after the
-- consumed value must be rejected at query time.

DROP USER IF EXISTS user_04836_valid_until;

-- A 20-digit all-digit literal is not fully consumed (19 digits parse as a nanosecond timestamp).
CREATE USER user_04836_valid_until VALID UNTIL '18446744327111802015'; -- { serverError BAD_ARGUMENTS }

-- Trailing alphabetic garbage after a well-formed datetime is rejected by the datetime parser itself.
CREATE USER user_04836_valid_until VALID UNTIL '2025-01-01 00:00:00 UTC junk'; -- { serverError CANNOT_PARSE_DATETIME }

-- The same rejections apply at the authentication-method (credential) level.
CREATE USER user_04836_valid_until IDENTIFIED WITH plaintext_password BY 'x' VALID UNTIL '18446744327111802015'; -- { serverError BAD_ARGUMENTS }

-- ... and to ALTER USER.
CREATE USER user_04836_valid_until;
ALTER USER user_04836_valid_until VALID UNTIL '18446744327111802015'; -- { serverError BAD_ARGUMENTS }
ALTER USER user_04836_valid_until VALID UNTIL '2025-01-01 00:00:00 UTC junk'; -- { serverError CANNOT_PARSE_DATETIME }

-- A literal the parser consumes in full - including trailing spaces - is still accepted.
ALTER USER user_04836_valid_until VALID UNTIL '2035-01-01 00:00:00 UTC ';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04836_valid_until';

-- Only the user deliberately created for the ALTER cases remains.
SELECT count() FROM system.users WHERE name = 'user_04836_valid_until';

DROP USER user_04836_valid_until;
