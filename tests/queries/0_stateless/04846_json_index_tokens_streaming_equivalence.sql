SET enable_json_type = 1;
SET log_queries = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_stream_direct;
CREATE TABLE json_stream_direct
(
    id UInt64,
    data JSON(s String, tags Array(Nullable(String)), max_dynamic_paths = 2, max_dynamic_types = 2),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_stream_direct;
INSERT INTO json_stream_direct
SELECT number, concat('{"s":"value-', toString(number), '","tags":["common",null],"dynamic":', toString(number), '}')
FROM numbers(4)
SETTINGS log_comment = '04846_stream_direct';
INSERT INTO json_stream_direct
SELECT number, concat('{"s":"', repeat('x', 100), '-', toString(number), '","tags":["other",""]}')
FROM numbers(4, 4)
SETTINGS log_comment = '04846_stream_direct';

SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['JSONPathValuesTextIndexInputRows'])
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04846_stream_direct';

SELECT groupArray(id) FROM json_stream_direct WHERE data.s = 'value-2'
SETTINGS force_data_skipping_indices = 'tokens';

SYSTEM START MERGES json_stream_direct;
OPTIMIZE TABLE json_stream_direct FINAL;
CHECK TABLE json_stream_direct SETTINGS check_query_single_value_result = 1;
DROP TABLE json_stream_direct;

CREATE TABLE json_stream_invalid
(
    data JSON,
    INDEX tokens tuple(data) TYPE text(tokenizer = jsonPathValues(64))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_stream_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), positions = 1)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_stream_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), preprocessor = lower(data))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_stream_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), postprocessor = lower(data))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_stream_mixed
(
    id UInt64,
    message String,
    data JSON(s String),
    INDEX message_tokens message TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1,
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_stream_mixed VALUES
    (1, 'ordinary needle', '{"s":"json one"}'),
    (2, 'ordinary other', '{"s":"json needle"}');
SELECT groupArray(id) FROM json_stream_mixed WHERE hasToken(message, 'needle')
SETTINGS force_data_skipping_indices = 'message_tokens';
SELECT groupArray(id) FROM json_stream_mixed WHERE data.s = 'json needle'
SETTINGS force_data_skipping_indices = 'json_tokens';
CHECK TABLE json_stream_mixed SETTINGS check_query_single_value_result = 1;
DROP TABLE json_stream_mixed;
