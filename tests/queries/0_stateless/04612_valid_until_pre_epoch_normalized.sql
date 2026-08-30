-- Tags: no-parallel
-- ^ creates a globally-named user; the flaky check runs the same test concurrently, so a fixed user
--   name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between parallel repetitions.

-- A pre-1970 (negative) `VALID UNTIL` deadline is already in the past, so the credential is expired.
-- It is normalized to the smallest expired instant (`1970-01-01 00:00:01`, `time_t` 1), the same way a
-- pre-epoch `VALID FOR` interval is clamped. This is required for correctness across a downgrade or
-- mixed-version replicated/on-disk access-entity reload: a negative deadline serialized as a datetime
-- string would be resolved to `0` (the "no expiration" sentinel) by an older reader, turning an expired
-- credential into a non-expiring one. Normalizing to `1` stores it as a plain Unix timestamp that every
-- reader interprets fail-closed. `toUInt32(valid_until[1])` therefore reports 1 (not 0, and not the
-- originally specified pre-epoch instant) below.

DROP USER IF EXISTS user_04612_valid_until;

-- Global VALID UNTIL well before the epoch (1950-01-01).
CREATE USER user_04612_valid_until VALID UNTIL '1950-01-01 00:00:00 UTC';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04612_valid_until';
DROP USER user_04612_valid_until;

-- ... at the authentication-method (credential) level.
CREATE USER user_04612_valid_until IDENTIFIED WITH plaintext_password BY 'x' VALID UNTIL '1950-01-01 00:00:00 UTC';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04612_valid_until';
DROP USER user_04612_valid_until;

-- ... one second before the epoch (still negative).
CREATE USER user_04612_valid_until VALID UNTIL '1969-12-31 23:59:59 UTC';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04612_valid_until';
DROP USER user_04612_valid_until;

-- ... and for ALTER USER.
CREATE USER user_04612_valid_until;
ALTER USER user_04612_valid_until VALID UNTIL '1950-01-01 00:00:00 UTC';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04612_valid_until';
DROP USER user_04612_valid_until;
