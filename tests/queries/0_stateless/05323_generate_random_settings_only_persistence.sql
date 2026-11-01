-- `SETTINGS` survive table creation without positional arguments to `generateRandom`.
DROP TABLE IF EXISTS t_generate_random_settings_only;
DROP TABLE IF EXISTS t_generate_random_settings_only_snapshot;

CREATE TABLE t_generate_random_settings_only AS generateRandom(SETTINGS null_ratio = 1);

CREATE TABLE t_generate_random_settings_only_snapshot ENGINE = Memory AS
SELECT create_table_query AS definition
FROM system.tables
WHERE database = currentDatabase() AND name = 't_generate_random_settings_only';

SELECT position(definition, 'SETTINGS null_ratio = 1') > 0
FROM t_generate_random_settings_only_snapshot;

-- Compare the complete definition, including its inferred columns, across `DETACH` and `ATTACH`.
DETACH TABLE t_generate_random_settings_only;
ATTACH TABLE t_generate_random_settings_only;

SELECT
    create_table_query = (SELECT definition FROM t_generate_random_settings_only_snapshot),
    position(create_table_query, 'SETTINGS null_ratio = 1') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_generate_random_settings_only';

DROP TABLE t_generate_random_settings_only;
DROP TABLE t_generate_random_settings_only_snapshot;
