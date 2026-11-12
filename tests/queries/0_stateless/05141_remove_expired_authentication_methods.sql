-- Tags: no-parallel
-- ^ creates a globally-named user; the flaky check runs the same test concurrently, so a fixed user
--   name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between parallel repetitions.

-- An expired authentication method can never accept a credential again, so every write to a user drops
-- the methods whose `VALID UNTIL` deadline has already passed and keeps all the others. Below,
-- `valid_until` is reported as an array of Unix timestamps (`0` means "no expiration"), which identifies
-- the surviving methods independently of the server time zone.

DROP USER IF EXISTS user_05141_expired_methods;

CREATE USER user_05141_expired_methods IDENTIFIED WITH plaintext_password BY 'never_expires';
ALTER USER user_05141_expired_methods ADD IDENTIFIED
    WITH plaintext_password BY 'expired_long_ago' VALID UNTIL '2020-01-01 00:00:00 UTC',
    plaintext_password BY 'expires_far_away' VALID UNTIL '2100-01-01 00:00:00 UTC';
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_expired_methods';

-- The methods a statement adds are kept even when they are already expired: writing an already expired
-- credential is a supported way to mark it as such. The next write drops it, and only it - here the
-- statement does not touch authentication at all.
ALTER USER user_05141_expired_methods DEFAULT DATABASE NONE;
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_expired_methods';

-- Rotating a short-lived credential never accumulates dead ones.
ALTER USER user_05141_expired_methods ADD IDENTIFIED WITH plaintext_password BY 'token_1' VALID UNTIL '2020-06-01 00:00:00 UTC';
ALTER USER user_05141_expired_methods ADD IDENTIFIED WITH plaintext_password BY 'token_2' VALID UNTIL '2020-07-01 00:00:00 UTC';
ALTER USER user_05141_expired_methods ADD IDENTIFIED WITH plaintext_password BY 'token_3' VALID UNTIL '2100-06-01 00:00:00 UTC';
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_expired_methods';

DROP USER user_05141_expired_methods;

-- A user-level `VALID UNTIL` is applied before the expired methods are dropped, so extending the deadline
-- of a user whose credentials have already lapsed still works and nothing is removed.
CREATE USER user_05141_expired_methods IDENTIFIED
    WITH plaintext_password BY 'a' VALID UNTIL '2020-01-01 00:00:00 UTC',
    plaintext_password BY 'b' VALID UNTIL '2020-02-01 00:00:00 UTC';
ALTER USER user_05141_expired_methods VALID UNTIL '2099-01-01 00:00:00 UTC';
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_expired_methods';

DROP USER user_05141_expired_methods;

-- A user whose every method is expired keeps them: a user with no authentication method at all is read
-- back as `no_password`, so dropping the last expired method would turn a lapsed credential into an
-- unauthenticated one. The user stays in the fail-closed state it is already in.
CREATE USER user_05141_expired_methods IDENTIFIED WITH plaintext_password BY 'a' VALID UNTIL '2020-01-01 00:00:00 UTC';
ALTER USER user_05141_expired_methods DEFAULT DATABASE NONE;
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_expired_methods';
SELECT arrayMap(x -> toString(x), auth_type) FROM system.users WHERE name = 'user_05141_expired_methods';

-- ... and giving it a new credential drops the expired one in the same statement.
ALTER USER user_05141_expired_methods ADD IDENTIFIED WITH plaintext_password BY 'b';
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_expired_methods';

DROP USER user_05141_expired_methods;
