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
        `items[].secret` Array(String),
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
    (1, '{"keep":"hit","skip":{"leaf":"hidden"},"skip_size":"prefix-hit","regex_token":"regex-hit","m":{"k":"v"},"items":[{"keep":"array-hit","secret":"array-secret"}],"items[].secret":["literal-secret"]}'),
    (2, '{"keep":"miss","skip":{"leaf":"other"},"skip_size":"prefix-miss","regex_token":"regex-miss","m":{"other":"x"},"items":[{"keep":"array-miss","secret":"array-other"}],"items[].secret":["literal-other"]}');

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
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE has(data.`items[].secret`, 'literal-secret')
SETTINGS force_data_skipping_indices = 'tokens';

SET mutations_sync = 2;
ALTER TABLE json_path_values_skip_paths DROP INDEX tokens;
ALTER TABLE json_path_values_skip_paths ADD INDEX literal_tokens data TYPE text(tokenizer = jsonPathValues(
    max_token_bytes = 64,
    skip_paths = ['items\\[\\].secret'])) GRANULARITY 1;
ALTER TABLE json_path_values_skip_paths MATERIALIZE INDEX literal_tokens;

SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE has(data.items[].secret, 'array-secret')
SETTINGS force_data_skipping_indices = 'literal_tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE has(data.`items[].secret`, 'literal-secret');
SELECT arraySort(groupArray(id)) FROM json_path_values_skip_paths WHERE has(data.`items[].secret`, 'literal-secret')
SETTINGS force_data_skipping_indices = 'literal_tokens'; -- { serverError INDEX_NOT_USED }

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
SET mutations_sync = 2;

DROP TABLE IF EXISTS json_path_values_include_paths;
DROP TABLE IF EXISTS json_path_values_include_paths_invalid;

CREATE TABLE json_path_values_include_paths
(
    id UInt64,
    data JSON(
        max_dynamic_paths = 0,
        request_id String,
        request String,
        created_at String,
        private_id String,
        message String,
        ignored Dynamic(max_types = 0),
        payload JSON(
            max_dynamic_paths = 0,
            ids JSON(max_dynamic_paths = 0, primary String, secret String),
            updated_at String,
            text String),
        items Array(JSON(max_dynamic_paths = 0, item_id String, name String)),
        m Map(String, String)),
    INDEX exact_tokens data TYPE text(tokenizer = jsonPathValues(
        max_token_bytes = 64,
        include_paths = ['request_id', 'payload.ids', 'items[].item_id', 'm'],
        skip_paths = ['payload.ids.secret'])) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_include_paths VALUES
    (1, '{"request_id":"req-1","request":"request-1","created_at":"time-1","private_id":"private-1","message":"message-1","ignored":[[1]],"payload":{"ids":{"primary":"primary-1","secret":"secret-1"},"updated_at":"updated-1","text":"text-1"},"items":[{"item_id":"item-1","name":"name-1"}],"m":{"k":"map-1"}}'),
    (2, '{"request_id":"req-2","request":"request-2","created_at":"time-2","private_id":"private-2","message":"message-2","ignored":[[2]],"payload":{"ids":{"primary":"primary-2","secret":"secret-2"},"updated_at":"updated-2","text":"text-2"},"items":[{"item_id":"item-2","name":"name-2"}],"m":{"k":"map-2"}}');

SELECT 'exact request_id', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'exact_tokens')
WHERE startsWith(hex(token), concat(hex('request_id'), '0000'));
SELECT 'exact nested', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'exact_tokens')
WHERE startsWith(hex(token), concat(hex('payload.ids.primary'), '0000'));
SELECT 'exact array JSON', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'exact_tokens')
WHERE startsWith(hex(token), concat(hex('items[].item_id'), '0000'));
SELECT 'exact map', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'exact_tokens')
WHERE startsWith(hex(token), concat(hex('m'), '0000'));
SELECT 'exact sibling excluded', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'exact_tokens')
WHERE startsWith(hex(token), concat(hex('request'), '0000'));
SELECT 'exact skip wins', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'exact_tokens')
WHERE startsWith(hex(token), concat(hex('payload.ids.secret'), '0000'));
SELECT 'exact unrelated dynamic excluded', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'exact_tokens')
WHERE startsWith(hex(token), concat(hex('ignored'), '0000'));

SELECT 'exact query', arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.request_id = 'req-2'
SETTINGS force_data_skipping_indices = 'exact_tokens';
SELECT 'exact nested query', arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.payload.ids.primary = 'primary-1'
SETTINGS force_data_skipping_indices = 'exact_tokens';
SELECT 'exact array query', arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE has(data.items[].item_id, 'item-2')
SETTINGS force_data_skipping_indices = 'exact_tokens';
SELECT 'exact map query', arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.m['k'] = 'map-1'
SETTINGS force_data_skipping_indices = 'exact_tokens';

SELECT arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.request = 'request-1';
SELECT arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.request = 'request-1'
SETTINGS force_data_skipping_indices = 'exact_tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.payload.ids.secret = 'secret-1';
SELECT arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.payload.ids.secret = 'secret-1'
SETTINGS force_data_skipping_indices = 'exact_tokens'; -- { serverError INDEX_NOT_USED }

ALTER TABLE json_path_values_include_paths DROP INDEX exact_tokens;
ALTER TABLE json_path_values_include_paths ADD INDEX regexp_tokens data TYPE text(tokenizer = jsonPathValues(
    max_token_bytes = 64,
    include_paths_regexp = ['(?:^|\\.)(?:.*_id|.*_at)$'],
    skip_paths_regexp = ['(?:^|\\.)private_'])) GRANULARITY 1;
ALTER TABLE json_path_values_include_paths MATERIALIZE INDEX regexp_tokens;

SELECT 'regexp direct', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'regexp_tokens')
WHERE startsWith(hex(token), concat(hex('created_at'), '0000'));
SELECT 'regexp nested', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'regexp_tokens')
WHERE startsWith(hex(token), concat(hex('payload.updated_at'), '0000'));
SELECT 'regexp array JSON', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'regexp_tokens')
WHERE startsWith(hex(token), concat(hex('items[].item_id'), '0000'));
SELECT 'regexp unrelated excluded', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'regexp_tokens')
WHERE startsWith(hex(token), concat(hex('message'), '0000'));
SELECT 'regexp skip wins', count() > 0 FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_include_paths', 'regexp_tokens')
WHERE startsWith(hex(token), concat(hex('private_id'), '0000'));

SELECT 'regexp query', arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.created_at = 'time-2'
SETTINGS force_data_skipping_indices = 'regexp_tokens';
SELECT 'regexp nested query', arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.payload.updated_at = 'updated-1'
SETTINGS force_data_skipping_indices = 'regexp_tokens';

SELECT arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.message = 'message-1';
SELECT arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.message = 'message-1'
SETTINGS force_data_skipping_indices = 'regexp_tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.private_id = 'private-1';
SELECT arraySort(groupArray(id)) FROM json_path_values_include_paths WHERE data.private_id = 'private-1'
SETTINGS force_data_skipping_indices = 'regexp_tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_include_paths;

CREATE TABLE json_path_values_include_paths_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(include_paths = 'request_id'))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_path_values_include_paths_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(include_paths = ['']))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_path_values_include_paths_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(include_paths_regexp = ['(']))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError CANNOT_COMPILE_REGEXP }

CREATE TABLE json_path_values_include_paths_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64, include_paths = []))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
