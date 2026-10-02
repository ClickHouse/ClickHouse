-- Tags: no-parallel
-- ^ creates globally-named users; the flaky check runs the same test concurrently, so a fixed user
--   name would collide (ACCESS_ENTITY_ALREADY_EXISTS) between parallel repetitions.

-- A user-level `VALID UNTIL` / `VALID FOR` clause applies to every authentication method of the user,
-- but a method that carries its own explicit clause in the same statement must keep its more specific
-- deadline. `updateUserFromQueryImpl` used to overwrite every method's deadline with the user-level one
-- unconditionally, silently discarding the per-method clause. The stored deadline is asserted through
-- `system.users` (its display in `SHOW CREATE USER` is server-time-zone-dependent). `valid_until[i]`
-- follows the authentication method declaration order.
-- 2035-01-01 00:00:00 UTC = 2051222400, 2040-01-01 00:00:00 UTC = 2208988800.

DROP USER IF EXISTS user_04611_a, user_04611_b, user_04611_c, user_04611_d;

-- CREATE USER: absolute user-level VALID UNTIL with an explicit method-level VALID UNTIL.
-- Method 1 (no own clause) takes the user-level 2035 deadline; method 2 keeps its own 2040 deadline.
CREATE USER user_04611_a
    VALID UNTIL '2035-01-01 00:00:00 UTC'
    IDENTIFIED WITH plaintext_password BY 'p1',
                    plaintext_password BY 'p2' VALID UNTIL '2040-01-01 00:00:00 UTC';
SELECT length(valid_until), toUInt32(valid_until[1]), toUInt32(valid_until[2])
FROM system.users WHERE name = 'user_04611_a';

-- CREATE USER: user-level VALID FOR with an explicit method-level VALID FOR.
-- Method 1 takes the user-level 1 day; method 2 keeps its own 30 days.
CREATE USER user_04611_b
    VALID FOR INTERVAL 1 DAY
    IDENTIFIED WITH plaintext_password BY 'p1',
                    plaintext_password BY 'p2' VALID FOR INTERVAL 30 DAY;
SELECT length(valid_until),
       valid_until[1] BETWEEN now() + INTERVAL 23 HOUR AND now() + INTERVAL 25 HOUR,
       valid_until[2] BETWEEN now() + INTERVAL 29 DAY AND now() + INTERVAL 31 DAY
FROM system.users WHERE name = 'user_04611_b';

-- ALTER USER ... VALID FOR ... ADD IDENTIFIED ... VALID FOR ...: the pre-existing method takes the
-- user-level deadline (1 day), while the newly added method keeps its own explicit deadline (30 days).
CREATE USER user_04611_c IDENTIFIED WITH plaintext_password BY 'p1';
ALTER USER user_04611_c
    VALID FOR INTERVAL 1 DAY
    ADD IDENTIFIED WITH plaintext_password BY 'p2' VALID FOR INTERVAL 30 DAY;
SELECT length(valid_until),
       valid_until[1] BETWEEN now() + INTERVAL 23 HOUR AND now() + INTERVAL 25 HOUR,
       valid_until[2] BETWEEN now() + INTERVAL 29 DAY AND now() + INTERVAL 31 DAY
FROM system.users WHERE name = 'user_04611_c';

-- Same as above for the absolute VALID UNTIL form: pre-existing method takes 2035, added method keeps 2040.
CREATE USER user_04611_d IDENTIFIED WITH plaintext_password BY 'p1';
ALTER USER user_04611_d
    VALID UNTIL '2035-01-01 00:00:00 UTC'
    ADD IDENTIFIED WITH plaintext_password BY 'p2' VALID UNTIL '2040-01-01 00:00:00 UTC';
SELECT length(valid_until), toUInt32(valid_until[1]), toUInt32(valid_until[2])
FROM system.users WHERE name = 'user_04611_d';

DROP USER user_04611_a, user_04611_b, user_04611_c, user_04611_d;
