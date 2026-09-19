-- Tags: no-parallel
-- ^ creates a globally-named user; the flaky check runs the same test concurrently, so a fixed user
--   name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between parallel repetitions.

-- For an explicit year of `0000`, `parseDateTimeBestEffort` sets `has_explicit_zero_year` and
-- substitutes the current (or previous) year, so a deadline of `0000-01-01 00:00:00 UTC` would
-- otherwise bypass the `1900-01-01` bound check in `getValidUntilFromAST` and resolve to a live,
-- non-expired deadline instead of being rejected. This holds for every date layout the best-effort
-- parser accepts, not only for the year-first one, so all of them are checked below.
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

-- The year field is not always the leading one: best-effort parsing accepts a day-first, slash- or
-- dot-separated date, and a compact date with no separators at all. Every spelling of a zero year is
-- rejected, not only `0000-...`.
ALTER USER user_04603_valid_until VALID UNTIL '01/01/0000'; -- { serverError BAD_ARGUMENTS }
ALTER USER user_04603_valid_until VALID UNTIL '01/01/0000 12:00:00'; -- { serverError BAD_ARGUMENTS }
ALTER USER user_04603_valid_until VALID UNTIL '01.01.0000 12:00:00 UTC'; -- { serverError BAD_ARGUMENTS }
ALTER USER user_04603_valid_until VALID UNTIL '00000101'; -- { serverError BAD_ARGUMENTS }
ALTER USER user_04603_valid_until VALID UNTIL '00000101000000'; -- { serverError BAD_ARGUMENTS }
ALTER USER user_04603_valid_until VALID UNTIL '000001'; -- { serverError BAD_ARGUMENTS }

-- Zero month/day components in year `0000` are zero-date placeholders for ordinary parsing, but an
-- explicit year of `0000` must still be rejected for VALID UNTIL.
ALTER USER user_04603_valid_until VALID UNTIL '0000-00-00 00:00:00 UTC'; -- { serverError BAD_ARGUMENTS }
ALTER USER user_04603_valid_until VALID UNTIL '0000-01-00 00:00:00 UTC'; -- { serverError BAD_ARGUMENTS }
ALTER USER user_04603_valid_until VALID UNTIL '0000-00-01 00:00:00 UTC'; -- { serverError BAD_ARGUMENTS }

-- A day-first date with a real year is still accepted, so the rejection above is not over-broad.
ALTER USER user_04603_valid_until VALID UNTIL '06/11/2040 08:03:20 Z';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04603_valid_until';
ALTER USER user_04603_valid_until VALID UNTIL 'infinity';

-- None of the rejected statements changed the stored deadline (no VALID UNTIL clause below).
SHOW CREATE USER user_04603_valid_until;

DROP USER user_04603_valid_until;
