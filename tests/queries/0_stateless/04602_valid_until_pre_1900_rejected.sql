-- Tags: no-parallel
-- ^ creates a globally-named user (the accepted-bound and ALTER cases below); the flaky check runs
--   the same test concurrently, so a fixed user name would collide (ACCESS_ENTITY_ALREADY_EXISTS)
--   between parallel repetitions.

-- The stored access entity encoding (`AuthenticationData::toAST`) writes a pre-epoch `VALID UNTIL`
-- deadline as a date-time string that older servers - whose `DateLUT` has no year earlier than 1900 -
-- can still parse. Accepting a deadline before that bound would make `SHOW CREATE USER` show a
-- different (clamped) deadline after a restart or replication round-trip than the one that was
-- originally specified, so it must be rejected instead.

DROP USER IF EXISTS user_04602_valid_until;

-- One second before the bound is rejected (global VALID UNTIL).
CREATE USER user_04602_valid_until VALID UNTIL '1899-12-31 23:59:59 UTC'; -- { serverError BAD_ARGUMENTS }

-- The bound itself is accepted.
CREATE USER user_04602_valid_until VALID UNTIL '1900-01-01 00:00:00 UTC';
DROP USER user_04602_valid_until;

-- The same rejection applies at the authentication-method (credential) level.
CREATE USER user_04602_valid_until IDENTIFIED WITH plaintext_password BY 'x' VALID UNTIL '1899-12-31 23:59:59 UTC'; -- { serverError BAD_ARGUMENTS }

-- ... and to ALTER USER.
CREATE USER user_04602_valid_until;
ALTER USER user_04602_valid_until VALID UNTIL '1899-12-31 23:59:59 UTC'; -- { serverError BAD_ARGUMENTS }

-- None of the rejected statements changed the stored deadline or left a user behind.
SELECT count() FROM system.users WHERE name = 'user_04602_valid_until';

DROP USER user_04602_valid_until;
