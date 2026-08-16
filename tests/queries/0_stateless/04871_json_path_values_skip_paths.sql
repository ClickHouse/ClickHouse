SET enable_json_type = 1;

DROP TABLE IF EXISTS json_path_values_skip_paths;

CREATE TABLE json_path_values_skip_paths
(
    id UInt64,
    data JSON(
        max_dynamic_paths = 0,
        keep String,
        `skip` JSON(max_dynamic_paths = 0, leaf String),
        skip_size String,
        regex_token String,
        m Map(String, String),
        items Array(JSON(max_dynamic_paths = 0, keep String, secret String))),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(
        max_token_bytes = 64,
        skip_paths = ['skip', 'm', 'items[].secret'],
        skip_paths_regexp = ['regex_'])) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_skip_paths VALUES
    (1, '{"keep":"hit","skip":{"leaf":"hidden"},"skip_size":"prefix-hit","regex_token":"regex-hit","m":{"k":"v"},"items":[{"keep":"array-hit","secret":"array-secret"}]}'),
    (2, '{"keep":"miss","skip":{"leaf":"other"},"skip_size":"prefix-miss","regex_token":"regex-miss","m":{"other":"x"},"items":[{"keep":"array-miss","secret":"array-other"}]}');

SELECT count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_skip_paths', 'tokens')
WHERE startsWith(hex(token), concat(hex('skip.leaf'), '0000'));
SELECT count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_skip_paths', 'tokens')
WHERE startsWith(hex(token), concat(hex('skip_size'), '0000'));
SELECT count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_skip_paths', 'tokens')
WHERE startsWith(hex(token), concat(hex('regex_token'), '0000'));
SELECT count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_skip_paths', 'tokens')
WHERE startsWith(hex(token), concat(hex('m'), '0000'));
SELECT count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_skip_paths', 'tokens')
WHERE startsWith(hex(token), concat(hex('items[].secret'), '0000'));

SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE data.keep = 'hit'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE data.skip_size = 'prefix-hit'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE has(data.items[].keep, 'array-hit')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE data.skip.leaf = 'hidden';
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE data.skip.leaf = 'hidden'
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE data.regex_token = 'regex-hit';
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE data.regex_token = 'regex-hit'
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE data.m['k'] = 'v';
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE data.m['k'] = 'v'
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE has(data.items[].secret, 'array-secret');
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE has(data.items[].secret, 'array-secret')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_skip_paths;

CREATE TABLE json_path_values_skip_paths_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(skip_paths = ['']))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_path_values_skip_paths_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(skip_paths_regexp = ['(']))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError CANNOT_COMPILE_REGEXP }

CREATE TABLE json_path_values_skip_paths_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64, skip_paths = []))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
