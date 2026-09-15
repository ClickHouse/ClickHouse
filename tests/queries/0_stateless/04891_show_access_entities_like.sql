-- Tags: no-parallel

DROP USER IF EXISTS show_like_u_alice, show_like_u_bob, show_like_u_AliceUpper;
DROP ROLE IF EXISTS show_like_r_admin, show_like_r_reader;
DROP SETTINGS PROFILE IF EXISTS show_like_p_fast, show_like_p_slow;

CREATE USER show_like_u_alice;
CREATE USER show_like_u_bob;
CREATE USER show_like_u_AliceUpper;
CREATE ROLE show_like_r_admin;
CREATE ROLE show_like_r_reader;
CREATE SETTINGS PROFILE show_like_p_fast;
CREATE SETTINGS PROFILE show_like_p_slow;

-- SHOW USERS LIKE: match all test users
SELECT '-- users like all';
SHOW USERS LIKE 'show\_like\_u\_%';

-- SHOW USERS LIKE: match only alice
SELECT '-- users like alice';
SHOW USERS LIKE 'show\_like\_u\_a%';

-- SHOW USERS ILIKE: case-insensitive matches both alice and AliceUpper
SELECT '-- users ilike alice';
SHOW USERS ILIKE 'SHOW\_LIKE\_U\_ALICE%';

-- SHOW ROLES LIKE: match all test roles
SELECT '-- roles like all';
SHOW ROLES LIKE 'show\_like\_r\_%';

-- SHOW ROLES LIKE: match only admin
SELECT '-- roles like admin';
SHOW ROLES LIKE 'show\_like\_r\_adm%';

-- SHOW ROLES ILIKE
SELECT '-- roles ilike admin';
SHOW ROLES ILIKE 'SHOW\_LIKE\_R\_ADM%';

-- SHOW SETTINGS PROFILES LIKE: match all test profiles
SELECT '-- profiles like all';
SHOW SETTINGS PROFILES LIKE 'show\_like\_p\_%';

-- SHOW SETTINGS PROFILES LIKE: match only fast
SELECT '-- profiles like fast';
SHOW SETTINGS PROFILES LIKE 'show\_like\_p\_f%';

-- SHOW SETTINGS PROFILES ILIKE
SELECT '-- profiles ilike fast';
SHOW SETTINGS PROFILES ILIKE 'SHOW\_LIKE\_P\_F%';

-- NOT LIKE: verify it parses and executes without error
SHOW USERS NOT LIKE 'show\_like\_u\_bob' FORMAT Null;
SHOW ROLES NOT LIKE 'show\_like\_r\_reader' FORMAT Null;
SHOW SETTINGS PROFILES NOT LIKE 'show\_like\_p\_slow' FORMAT Null;

-- NOT ILIKE: verify it parses and executes without error
SHOW USERS NOT ILIKE '%BOB%' FORMAT Null;

-- Cleanup
DROP USER show_like_u_alice, show_like_u_bob, show_like_u_AliceUpper;
DROP ROLE show_like_r_admin, show_like_r_reader;
DROP SETTINGS PROFILE show_like_p_fast, show_like_p_slow;
