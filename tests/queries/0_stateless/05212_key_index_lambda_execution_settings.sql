SET short_circuit_function_evaluation = 'disable';
CREATE TABLE lambda_key_settings (arr Array(UInt64))
ENGINE = MergeTree ORDER BY arrayMap(x -> if(x = 0, 0, intDiv(1, x)), arr);
ALTER TABLE lambda_key_settings COMMENT COLUMN arr 'Rebuild key metadata';
INSERT INTO lambda_key_settings SETTINGS short_circuit_function_evaluation = 'force_enable' VALUES ([0, 1]);
SELECT arr FROM lambda_key_settings;
DROP TABLE lambda_key_settings;

CREATE TABLE lambda_index_settings
(
    id UInt64,
    arr Array(UInt64),
    INDEX idx arrayMap(x -> if(x = 0, 0, intDiv(1, x)), arr) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;
ALTER TABLE lambda_index_settings COMMENT COLUMN arr 'Rebuild index metadata';
INSERT INTO lambda_index_settings SETTINGS short_circuit_function_evaluation = 'force_enable' VALUES (1, [0, 1]);
ALTER TABLE lambda_index_settings MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT arr FROM lambda_index_settings;
DROP TABLE lambda_index_settings;
