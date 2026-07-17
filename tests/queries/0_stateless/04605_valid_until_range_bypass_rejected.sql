-- Tags: no-parallel
-- ^ creates a globally-named user (the accepted-bound and ALTER cases below); the flaky check runs
--   the same test concurrently, so a fixed user name would collide (ACCESS_ENTITY_ALREADY_EXISTS)
--   between parallel repetitions.

-- `getValidUntilFromAST` range-checks an absolute `VALID UNTIL` deadline. Two ways of specifying an
-- out-of-range instant used to slip past those checks:
--   1. An explicit year `0000` with a leading space: `parseDateTimeBestEffort` skips the space before
--      it reads the year, so ` 0000-...` reached the "year omitted" fallback and became a current-year
--      deadline instead of being rejected.
--   2. An in-range date with an explicit time-zone offset that crosses the year-9999 boundary: the
--      instant is stored past the `DateLUT` ceiling, but `SHOW CREATE USER` displays it clamped back to
--      `9999-12-31 23:59:59`, so the credential outlived the shown deadline.
-- Both must be rejected at query time.

DROP USER IF EXISTS user_04605_valid_until;

-- Year `0000` with a leading space is rejected (global VALID UNTIL).
CREATE USER user_04605_valid_until VALID UNTIL ' 0000-01-01 00:00:00 UTC'; -- { serverError BAD_ARGUMENTS }

-- A deadline one hour past the year-9999 ceiling (via a time-zone offset) is rejected.
CREATE USER user_04605_valid_until VALID UNTIL '9999-12-31 23:59:59 -01:00'; -- { serverError BAD_ARGUMENTS }

-- The latest representable deadline itself is accepted (its exact round-trip value is asserted by the
-- gtest `ValidUntilAttachEncoding.HandEditedDeadlineBeyondMaxFailsToLoad`; `SHOW CREATE USER` /
-- `system.users` would display it time-zone-dependently or clamped, so it is not printed here).
CREATE USER user_04605_valid_until VALID UNTIL '9999-12-31 23:59:59 UTC';
DROP USER user_04605_valid_until;

-- The same rejections apply at the authentication-method (credential) level.
CREATE USER user_04605_valid_until IDENTIFIED WITH plaintext_password BY 'x' VALID UNTIL ' 0000-01-01 00:00:00 UTC'; -- { serverError BAD_ARGUMENTS }
CREATE USER user_04605_valid_until IDENTIFIED WITH plaintext_password BY 'x' VALID UNTIL '9999-12-31 23:59:59 -01:00'; -- { serverError BAD_ARGUMENTS }

-- ... and to ALTER USER.
CREATE USER user_04605_valid_until;
ALTER USER user_04605_valid_until VALID UNTIL ' 0000-01-01 00:00:00 UTC'; -- { serverError BAD_ARGUMENTS }
ALTER USER user_04605_valid_until VALID UNTIL '9999-12-31 23:59:59 -01:00'; -- { serverError BAD_ARGUMENTS }

-- Only the user deliberately created for the ALTER cases remains; none of the rejected statements
-- created an extra user or left the deadline in a broken state.
SELECT count() FROM system.users WHERE name = 'user_04605_valid_until';

DROP USER user_04605_valid_until;
