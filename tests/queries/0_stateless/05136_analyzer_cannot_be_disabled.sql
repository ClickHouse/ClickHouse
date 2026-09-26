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

-- Disabling it is accepted and replaced with `1`, under either name and in every form, so that
-- queries, sessions, settings profiles and client configurations that still carry
-- `enable_analyzer = 0` keep working after an upgrade.
SET enable_analyzer = 0;
SET allow_experimental_analyzer = 0;
SET enable_analyzer = false;
SELECT toUInt8(getSetting('enable_analyzer')), toUInt8(getSetting('allow_experimental_analyzer'));
SELECT toUInt8(getSetting('enable_analyzer')) SETTINGS enable_analyzer = 0;
SELECT toUInt8(getSetting('allow_experimental_analyzer')) SETTINGS allow_experimental_analyzer = 0;
INSERT INTO FUNCTION null('x UInt8') SETTINGS enable_analyzer = 0 SELECT 1;

-- A view keeps working too, and its stored definition does not carry the disabling value.
DROP VIEW IF EXISTS v_05136;
CREATE VIEW v_05136 AS SELECT toUInt8(getSetting('enable_analyzer')) AS x SETTINGS enable_analyzer = 0;
SELECT x FROM v_05136;
SELECT position(create_table_query, 'enable_analyzer = 0') = 0 FROM system.tables WHERE database = currentDatabase() AND name = 'v_05136';
DROP VIEW v_05136;

-- An access entity can be created with the disabling value, too, and stores `1` instead.
DROP SETTINGS PROFILE IF EXISTS profile_05136;
CREATE SETTINGS PROFILE profile_05136 SETTINGS enable_analyzer = 0;
SELECT value FROM system.settings_profile_elements WHERE profile_name = 'profile_05136';
ALTER SETTINGS PROFILE profile_05136 SETTINGS allow_experimental_analyzer = 0;
SELECT value FROM system.settings_profile_elements WHERE profile_name = 'profile_05136';
ALTER SETTINGS PROFILE profile_05136 MODIFY SETTINGS enable_analyzer = 0;
SELECT value FROM system.settings_profile_elements WHERE profile_name = 'profile_05136';
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
