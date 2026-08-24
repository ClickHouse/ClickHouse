-- Tags: no-parallel
-- ^ creates a globally-named user; the flaky check runs the same test concurrently, so a fixed user
--   name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between parallel repetitions.

-- VALID FOR <interval> is a shortcut for VALID UNTIL now + interval, resolved at query execution time.

DROP USER IF EXISTS user_04537_valid_for;

-- VALID FOR at the user level (applies to the implicit no_password authentication method).
CREATE USER user_04537_valid_for VALID FOR INTERVAL 1 DAY;
SELECT valid_until[1] BETWEEN now() + INTERVAL 23 HOUR AND now() + INTERVAL 25 HOUR
FROM system.users WHERE name = 'user_04537_valid_for';
DROP USER user_04537_valid_for;

-- VALID FOR together with an authentication method (credential level).
CREATE USER user_04537_valid_for IDENTIFIED WITH plaintext_password BY 'x' VALID FOR INTERVAL 2 DAY;
SELECT valid_until[1] BETWEEN now() + INTERVAL 47 HOUR AND now() + INTERVAL 49 HOUR
FROM system.users WHERE name = 'user_04537_valid_for';

-- ALTER USER ... VALID FOR recomputes the deadline.
ALTER USER user_04537_valid_for VALID FOR INTERVAL 1 YEAR;
SELECT valid_until[1] BETWEEN now() + INTERVAL 364 DAY AND now() + INTERVAL 367 DAY
FROM system.users WHERE name = 'user_04537_valid_for';

-- A negative interval yields a deadline in the past (an already-expired credential).
ALTER USER user_04537_valid_for VALID FOR INTERVAL -1 DAY;
SELECT valid_until[1] < now()
FROM system.users WHERE name = 'user_04537_valid_for';

DROP USER user_04537_valid_for;

-- Each authentication method can carry its own VALID FOR.
CREATE USER user_04537_valid_for
    IDENTIFIED WITH plaintext_password BY 'a' VALID FOR INTERVAL 1 DAY,
                    plaintext_password BY 'b' VALID FOR INTERVAL 10 DAY;
SELECT
    arraySort(valid_until)[1] BETWEEN now() + INTERVAL 23 HOUR AND now() + INTERVAL 25 HOUR,
    arraySort(valid_until)[2] BETWEEN now() + INTERVAL 9 DAY AND now() + INTERVAL 11 DAY
FROM system.users WHERE name = 'user_04537_valid_for';

-- A combination of intervals is accepted.
ALTER USER user_04537_valid_for VALID FOR INTERVAL 1 DAY + INTERVAL 12 HOUR;
SELECT valid_until[1] BETWEEN now() + INTERVAL 35 HOUR AND now() + INTERVAL 37 HOUR
FROM system.users WHERE name = 'user_04537_valid_for';

DROP USER user_04537_valid_for;

-- All VALID FOR clauses of one statement resolve against a single reference time, so two identical
-- intervals produce identical deadlines instead of two independently sampled `now` values.
CREATE USER user_04537_valid_for
    IDENTIFIED WITH plaintext_password BY 'a' VALID FOR INTERVAL 1 DAY,
                    plaintext_password BY 'b' VALID FOR INTERVAL 1 DAY;
SELECT length(valid_until) = 2 AND valid_until[1] = valid_until[2]
FROM system.users WHERE name = 'user_04537_valid_for';
DROP USER user_04537_valid_for;
