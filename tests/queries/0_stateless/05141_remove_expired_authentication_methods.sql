-- Tags: no-parallel
-- ^ creates a globally-named user; the flaky check runs the same test concurrently, so a fixed user
--   name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between parallel repetitions.

-- `ALTER USER ... REMOVE EXPIRED AUTHENTICATION METHODS` drops the authentication methods whose
-- `VALID UNTIL` deadline has already passed and keeps every other one, including the non-expiring ones.
-- Below, `valid_until` is reported as an array of Unix timestamps (`0` means "no expiration"), which
-- identifies the surviving methods independently of the server time zone.

DROP USER IF EXISTS user_05141_remove_expired;

CREATE USER user_05141_remove_expired IDENTIFIED WITH plaintext_password BY 'never_expires';
ALTER USER user_05141_remove_expired ADD IDENTIFIED
    WITH plaintext_password BY 'expired_long_ago' VALID UNTIL '2020-01-01 00:00:00 UTC',
    plaintext_password BY 'expires_far_away' VALID UNTIL '2100-01-01 00:00:00 UTC';
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_remove_expired';

-- Only the 2020 deadline is in the past, so only that method is dropped.
ALTER USER user_05141_remove_expired REMOVE EXPIRED AUTHENTICATION METHODS;
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_remove_expired';

-- Nothing is expired any more, so repeating the statement changes nothing.
ALTER USER user_05141_remove_expired REMOVE EXPIRED AUTHENTICATION METHODS;
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_remove_expired';

DROP USER user_05141_remove_expired;

-- The removal combines with `ADD IDENTIFIED`, so a dead credential can be replaced atomically. It acts
-- on the methods the user has when the statement starts, and is applied before the new ones are appended
-- (and before `max_authentication_methods_per_user` is checked), so an already at the limit user can
-- purge and add in one statement.
CREATE USER user_05141_remove_expired IDENTIFIED WITH plaintext_password BY 'a' VALID UNTIL '2020-01-01 00:00:00 UTC';
ALTER USER user_05141_remove_expired REMOVE EXPIRED AUTHENTICATION METHODS
    ADD IDENTIFIED WITH plaintext_password BY 'b' VALID UNTIL '2100-01-01 00:00:00 UTC';
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_remove_expired';

-- The order of the two clauses does not matter, and a method the same statement adds is not subject to
-- the removal even when its own deadline is already in the past.
ALTER USER user_05141_remove_expired
    ADD IDENTIFIED WITH plaintext_password BY 'c' VALID UNTIL '2020-01-01 00:00:00 UTC'
    REMOVE EXPIRED AUTHENTICATION METHODS;
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_remove_expired';

DROP USER user_05141_remove_expired;

-- A user-level `VALID UNTIL` is applied to the methods that survive the removal, so an already expired
-- method is dropped rather than revived with the new deadline.
CREATE USER user_05141_remove_expired IDENTIFIED
    WITH plaintext_password BY 'a' VALID UNTIL '2020-01-01 00:00:00 UTC',
    plaintext_password BY 'b' VALID UNTIL '2100-01-01 00:00:00 UTC';
ALTER USER user_05141_remove_expired VALID UNTIL '2099-01-01 00:00:00 UTC' REMOVE EXPIRED AUTHENTICATION METHODS;
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_remove_expired';

DROP USER user_05141_remove_expired;

-- Removing every method would leave the user with no way to authenticate at all, which is rejected
-- instead of silently falling back to `no_password`. The user is left untouched.
CREATE USER user_05141_remove_expired IDENTIFIED WITH plaintext_password BY 'a' VALID UNTIL '2020-01-01 00:00:00 UTC';
ALTER USER user_05141_remove_expired REMOVE EXPIRED AUTHENTICATION METHODS; -- { serverError BAD_ARGUMENTS }
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_remove_expired';

-- ... unless the same statement provides a replacement.
ALTER USER user_05141_remove_expired REMOVE EXPIRED AUTHENTICATION METHODS
    ADD IDENTIFIED WITH plaintext_password BY 'b';
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05141_remove_expired';

-- There is nothing to remove in `CREATE USER`, so the clause is `ALTER USER`-only.
CREATE USER user_05141_remove_expired_new REMOVE EXPIRED AUTHENTICATION METHODS; -- { clientError SYNTAX_ERROR }

-- It cannot be combined with the clauses that replace every authentication method, in either order.
ALTER USER user_05141_remove_expired REMOVE EXPIRED AUTHENTICATION METHODS RESET AUTHENTICATION METHODS TO NEW; -- { clientError SYNTAX_ERROR }
ALTER USER user_05141_remove_expired RESET AUTHENTICATION METHODS TO NEW REMOVE EXPIRED AUTHENTICATION METHODS; -- { clientError SYNTAX_ERROR }
ALTER USER user_05141_remove_expired REMOVE EXPIRED AUTHENTICATION METHODS IDENTIFIED WITH plaintext_password BY 'x'; -- { clientError SYNTAX_ERROR }
ALTER USER user_05141_remove_expired IDENTIFIED WITH plaintext_password BY 'x' REMOVE EXPIRED AUTHENTICATION METHODS; -- { clientError SYNTAX_ERROR }
ALTER USER user_05141_remove_expired REMOVE EXPIRED AUTHENTICATION METHODS NOT IDENTIFIED; -- { clientError SYNTAX_ERROR }
ALTER USER user_05141_remove_expired NOT IDENTIFIED REMOVE EXPIRED AUTHENTICATION METHODS; -- { clientError SYNTAX_ERROR }

-- Repeating the clause is a syntax error too.
ALTER USER user_05141_remove_expired REMOVE EXPIRED AUTHENTICATION METHODS REMOVE EXPIRED AUTHENTICATION METHODS; -- { clientError SYNTAX_ERROR }

DROP USER user_05141_remove_expired;
