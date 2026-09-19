-- Tags: no-parallel
-- ^ creates a globally-named user; the flaky check runs the same test concurrently, so a fixed user
--   name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between parallel repetitions.

-- `AuthenticationData::fromAST` builds the `no_authentication` method in a dedicated branch, which used
-- to return without setting the already-resolved deadline, so `VALID UNTIL` / `VALID FOR` was silently
-- dropped for this authentication type only (missing from `SHOW CREATE USER` and never enforced).
-- The deadline display in `SHOW CREATE USER` depends on the server time zone, so the stored deadline is
-- asserted through `system.users` instead.

DROP USER IF EXISTS user_04606_no_auth;

-- An absolute VALID UNTIL survives with the exact deadline (2035-01-01 00:00:00 UTC = 2051222400).
CREATE USER user_04606_no_auth IDENTIFIED WITH no_authentication VALID UNTIL '2035-01-01 00:00:00 UTC';
SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_04606_no_auth';
DROP USER user_04606_no_auth;

-- VALID FOR resolves to a deadline the interval away from now.
CREATE USER user_04606_no_auth IDENTIFIED WITH no_authentication VALID FOR INTERVAL 1 DAY;
SELECT valid_until[1] BETWEEN now() + INTERVAL 23 HOUR AND now() + INTERVAL 25 HOUR
FROM system.users WHERE name = 'user_04606_no_auth';

-- ALTER USER recomputes the deadline; a negative interval yields an already-expired credential.
ALTER USER user_04606_no_auth IDENTIFIED WITH no_authentication VALID FOR INTERVAL -1 SECOND;
SELECT valid_until[1] <= now() FROM system.users WHERE name = 'user_04606_no_auth';

DROP USER user_04606_no_auth;
