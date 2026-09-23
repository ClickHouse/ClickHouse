-- Tags: no-parallel
-- no-parallel: a settings profile is server-global rather than per-database, and its name cannot be
-- made unique per run: query parameters are not accepted in access-entity DDL. So this test is not
-- safe against a concurrent copy of itself - which is how the flaky check runs it.

-- The analyzer is mandatory since 26.9: `enable_analyzer` (canonically `allow_experimental_analyzer`)
-- is an obsolete setting frozen at its only supported value.

-- Setting it to that value is still accepted, under either name.
SET enable_analyzer = 1;
SET allow_experimental_analyzer = true;
SELECT toUInt8(getSetting('enable_analyzer')), toUInt8(getSetting('allow_experimental_analyzer'));

-- Disabling it is refused, under either name and in every form. A `SET` is refused by the server; a
-- `SETTINGS` clause of a query is refused before the query is sent, because the client applies such a
-- clause to its own session first (`InterpreterSetQuery::applySettingsFromQuery`) - hence
-- `clientError` for those. Both raise `SETTING_CONSTRAINT_VIOLATION`.
SET enable_analyzer = 0; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SET allow_experimental_analyzer = 0; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SET enable_analyzer = false; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SELECT 1 SETTINGS enable_analyzer = 0; -- { clientError SETTING_CONSTRAINT_VIOLATION }
SELECT 1 SETTINGS allow_experimental_analyzer = 0; -- { clientError SETTING_CONSTRAINT_VIOLATION }
INSERT INTO FUNCTION null('x UInt8') SETTINGS enable_analyzer = 0 SELECT 1; -- { clientError SETTING_CONSTRAINT_VIOLATION }
CREATE VIEW v_05136 AS SELECT 1 SETTINGS enable_analyzer = 0; -- { clientError SETTING_CONSTRAINT_VIOLATION }

-- A refused change leaves the setting alone.
SELECT toUInt8(getSetting('enable_analyzer'));

-- An access entity cannot carry the disabled value either.
DROP SETTINGS PROFILE IF EXISTS profile_05136;
CREATE SETTINGS PROFILE profile_05136 SETTINGS enable_analyzer = 0; -- { serverError SETTING_CONSTRAINT_VIOLATION }
CREATE SETTINGS PROFILE profile_05136 SETTINGS allow_experimental_analyzer = 0; -- { serverError SETTING_CONSTRAINT_VIOLATION }
CREATE SETTINGS PROFILE profile_05136 SETTINGS enable_analyzer = 1;
DROP SETTINGS PROFILE profile_05136;

-- `compatibility` with a version older than the one that made the analyzer the default used to revert
-- the setting. An obsolete setting is left alone by `compatibility`, so it does not anymore.
SET compatibility = '23.8';
SELECT toUInt8(getSetting('enable_analyzer')), toUInt8(getSetting('allow_experimental_analyzer'));
SET compatibility = DEFAULT;

-- The deprecation is visible in the settings introspection.
SELECT name, type, value, is_obsolete, tier, alias_for
FROM system.settings
WHERE name IN ('enable_analyzer', 'allow_experimental_analyzer')
ORDER BY name;
