-- Tags: no-parallel
-- ^ creates a globally-named user; the flaky check runs the same test concurrently, so a fixed user
--   name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between parallel repetitions.

-- `VALID UNTIL '1970-01-01 00:00:00 UTC'` parses to the Unix epoch (`time_t` 0), which is the internal
-- sentinel for "no expiration": `AuthenticationData::toAST` serializes the deadline only when it is
-- non-zero, and the authentication check skips the expiration test when `valid_until` is 0. Only the
-- literal `infinity` is meant to disable expiration; a real deadline at the epoch means the credential
-- is expired, so it must be stored as the smallest expired instant (1) instead of collapsing to "no
-- expiration". `toUInt32(valid_until[1])` therefore reports 1 (not 0) below.

DROP USER IF EXISTS user_04604_valid_until;

-- Global VALID UNTIL at the Unix epoch.
CREATE USER user_04604_valid_until VALID UNTIL '1970-01-01 00:00:00 UTC';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04604_valid_until';
DROP USER user_04604_valid_until;

-- ... at the authentication-method (credential) level.
CREATE USER user_04604_valid_until IDENTIFIED WITH plaintext_password BY 'x' VALID UNTIL '1970-01-01 00:00:00 UTC';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04604_valid_until';
DROP USER user_04604_valid_until;

-- ... and for ALTER USER.
CREATE USER user_04604_valid_until;
ALTER USER user_04604_valid_until VALID UNTIL '1970-01-01 00:00:00 UTC';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04604_valid_until';
DROP USER user_04604_valid_until;

-- `infinity` still means "no expiration" (deadline 0), so no VALID UNTIL clause is stored.
CREATE USER user_04604_valid_until VALID UNTIL 'infinity';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04604_valid_until';
SHOW CREATE USER user_04604_valid_until;
DROP USER user_04604_valid_until;
