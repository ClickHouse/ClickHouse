-- `GenerateRandom` reads three settings and folds them into a `GenerateRandomOptions` the storage keeps.
-- The storage keeps the settings object as well, so a setting reports its own value and where it came
-- from, rather than the option it was folded into.

DROP TABLE IF EXISTS generate_random_settings;
CREATE TABLE generate_random_settings (a UInt64) ENGINE = GenerateRandom(1)
    SETTINGS max_json_depth = 7, null_ratio = 0.5;

SELECT '-- the definition is recorded, and what it leaves out is the compiled-in default';
SELECT name, value, `default`, changed, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'generate_random_settings' ORDER BY name;

SELECT '-- the engine describes the same settings, at the values a table created now would get';
SELECT name, value, `default`, changed, source FROM system.engine_settings
WHERE engine = 'GenerateRandom' ORDER BY name;

DROP TABLE generate_random_settings;
