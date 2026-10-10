SET use_skip_indexes = 1;

DROP TABLE IF EXISTS json_path_values_shadowed_column;
CREATE TABLE json_path_values_shadowed_column
(
    j JSON(m Map(String, String)),
    `j.m.key_nokey` String,
    INDEX idx j TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO json_path_values_shadowed_column VALUES ('{"m":{"abc":"x"}}', 'hello');

SELECT 'physical column', count() FROM json_path_values_shadowed_column WHERE `j.m.key_nokey` = 'hello';
SELECT 'physical column, no index', count() FROM json_path_values_shadowed_column WHERE `j.m.key_nokey` = 'hello' SETTINGS use_skip_indexes = 0;
SELECT 'physical column, IN', count() FROM json_path_values_shadowed_column WHERE `j.m.key_nokey` IN ('hello', 'other');

DROP TABLE IF EXISTS json_path_values_shadowed_path;
CREATE TABLE json_path_values_shadowed_path
(
    j JSON(m Map(String, String), `m.key_nokey` String),
    INDEX idx j TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO json_path_values_shadowed_path VALUES ('{"m.key_nokey":"hello"}');

SELECT 'typed JSON path', count() FROM json_path_values_shadowed_path WHERE j.`m.key_nokey` = 'hello';
SELECT 'typed JSON path, no index', count() FROM json_path_values_shadowed_path WHERE j.`m.key_nokey` = 'hello' SETTINGS use_skip_indexes = 0;
SELECT 'typed JSON path, IN', count() FROM json_path_values_shadowed_path WHERE j.`m.key_nokey` IN ('hello', 'other');

SELECT 'genuine map key', count() FROM json_path_values_shadowed_path WHERE j.m.key_zzz = 'missing'
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_path_values_shadowed_column;
DROP TABLE json_path_values_shadowed_path;
