-- Tags: no-parallel
-- A settings profile is server-global rather than per-database, and its name cannot be made unique
-- per run: query parameters are not accepted in access-entity DDL. So this test is not safe against
-- a concurrent copy of itself - which is how the flaky check runs it - and has to be sequential.

-- A `SET` statement that changes `profile` installs a new constraint set halfway through itself.
-- Everything assigned or reset after that change must be checked against the new constraints, so
-- that one statement cannot do what the same two statements in sequence are not allowed to do.

DROP SETTINGS PROFILE IF EXISTS profile_05021;

CREATE SETTINGS PROFILE profile_05021 SETTINGS
    max_execution_time = 10 CONST,
    SQL_tenant_id = 1 CONST;

SELECT '-- an explicit value after the profile change is checked against the new constraints';
SET profile = 'profile_05021', max_execution_time = 999; -- { serverError 452 }

SELECT '-- and so is a reset';
SET profile = 'profile_05021', max_execution_time = DEFAULT; -- { serverError 452 }
SET profile = 'profile_05021', SQL_tenant_id = DEFAULT; -- { serverError 452 }

-- The profile is what would have installed `SQL_tenant_id`, so the setting being unknown proves the
-- rejected statements above left the session alone instead of applying the profile and dropping
-- their own tail.
SELECT '-- a rejected statement leaves the profile unapplied';
SELECT getSetting('SQL_tenant_id'); -- { serverError 115 }

-- An assignment placed *before* the profile change is not checked against the profile's constraints,
-- because it takes effect before them. A custom setting on purpose: `clickhouse-client` keeps the
-- settings a successful `SET` established and re-sends them with every later query, so a built-in
-- setting assigned here and then overridden by the profile would be rejected while the next query's
-- settings packet is received.
SELECT '-- an assignment before the profile change is not checked against the new constraints';
SET SQL_before_05021 = 7, profile = 'profile_05021';
SELECT getSetting('SQL_before_05021');

SELECT '-- the constraints the profile installed are in force afterwards';
SET max_execution_time = 999; -- { serverError 452 }
SET max_execution_time = DEFAULT; -- { serverError 452 }
SELECT getSetting('max_execution_time'), getSetting('SQL_tenant_id');

SELECT '-- assigning the value the profile installed is a no-op and stays allowed';
SET profile = 'profile_05021', max_execution_time = 10;

SELECT '-- an unconstrained setting after the profile change still applies';
SET profile = 'profile_05021', SQL_unconstrained_05021 = 1234;
SELECT getSetting('SQL_unconstrained_05021');

DROP SETTINGS PROFILE profile_05021;
