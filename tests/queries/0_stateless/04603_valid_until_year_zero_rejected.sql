-- Tags: no-parallel
-- ^ creates a globally-named user; the flaky check runs the same test concurrently, so a fixed user
--   name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between parallel repetitions.

-- `parseDateTimeBestEffort` treats an explicit year of `0000` the same as "year not specified" and
-- silently substitutes the current year, so a deadline of `0000-01-01 00:00:00 UTC` would otherwise
-- bypass the `1900-01-01` bound check in `getValidUntilFromAST` and resolve to a live, non-expired
-- deadline instead of being rejected.
-- No user is ever created by a rejected statement (it fails before the user is stored, except for
-- the ALTER USER case below, which creates the user first without VALID UNTIL).

DROP USER IF EXISTS user_04603_valid_until;

-- An explicit year of `0000` is rejected (global VALID UNTIL).
CREATE USER user_04603_valid_until VALID UNTIL '0000-01-01 00:00:00 UTC'; -- { serverError BAD_ARGUMENTS }

-- ... and at the authentication-method (credential) level.
CREATE USER user_04603_valid_until IDENTIFIED WITH plaintext_password BY 'x' VALID UNTIL '0000-06-15 12:00:00 UTC'; -- { serverError BAD_ARGUMENTS }

-- ... and for ALTER USER.
CREATE USER user_04603_valid_until;
ALTER USER user_04603_valid_until VALID UNTIL '0000-12-31 23:59:59 UTC'; -- { serverError BAD_ARGUMENTS }

-- None of the rejected statements changed the stored deadline (no VALID UNTIL clause below).
SHOW CREATE USER user_04603_valid_until;

DROP USER user_04603_valid_until;
