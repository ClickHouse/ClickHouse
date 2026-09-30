-- VALID FOR accepts only interval expressions (e.g. INTERVAL 1 DAY). A bare number such as
-- `VALID FOR 365` would otherwise be silently resolved as `plus(DateTime64, Number)` = addSeconds,
-- setting a 365-second lifetime instead of failing, so non-interval values must be rejected.
-- No user is ever created (every statement below fails while evaluating the clause, before the user
-- is stored), so the test keeps no global state and stays parallel-safe without a no-parallel tag.

DROP USER IF EXISTS user_04601_valid_for;

-- A bare number is not an interval (global VALID FOR).
CREATE USER user_04601_valid_for VALID FOR 365; -- { serverError BAD_ARGUMENTS }

-- A numeric arithmetic expression is not an interval either.
CREATE USER user_04601_valid_for VALID FOR 100 + 200; -- { serverError BAD_ARGUMENTS }

-- The same rejection applies at the authentication-method (credential) level.
CREATE USER user_04601_valid_for IDENTIFIED WITH plaintext_password BY 'x' VALID FOR 42; -- { serverError BAD_ARGUMENTS }

-- None of the rejected statements created a user.
SELECT count() FROM system.users WHERE name = 'user_04601_valid_for';
