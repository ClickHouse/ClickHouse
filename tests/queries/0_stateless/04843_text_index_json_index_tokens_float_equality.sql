SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_index_tokens_float_equality;
CREATE TABLE json_index_tokens_float_equality
(
    id UInt64,
    data JSON(f32 Float32, f64 Float64, nonzero Float64, floats Array(Float64), nested_floats Array(Tuple(Float64))),
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(64))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_float_equality;
INSERT INTO json_index_tokens_float_equality VALUES
    (1, '{"f32":-0.0,"f64":-0.0,"nonzero":1.5,"floats":[-0.0],"nested_floats":[[-0.0]]}'),
    (2, '{"f32":0.0,"f64":0.0,"nonzero":2.5,"floats":[0.0],"nested_floats":[[0.0]]}');

SELECT 'signed zero';
SELECT groupArray(id) FROM json_index_tokens_float_equality WHERE data.f32 = toFloat32(0.0)
SETTINGS force_data_skipping_indices = 'json_tokens';
SELECT groupArray(id) FROM json_index_tokens_float_equality WHERE data.f64 = toFloat64(-0.0)
SETTINGS force_data_skipping_indices = 'json_tokens';

SELECT 'nonzero float';
SELECT groupArray(id) FROM json_index_tokens_float_equality WHERE data.nonzero = toFloat64(1.5)
SETTINGS force_data_skipping_indices = 'json_tokens';

SELECT 'structured float equality fallback';
SELECT groupArray(id) FROM json_index_tokens_float_equality WHERE data.floats = [toFloat64(0.0)];
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_float_equality WHERE data.floats = [toFloat64(0.0)]
)
WHERE position(explain, '__text_index') > 0;
SELECT groupArray(id) FROM json_index_tokens_float_equality WHERE data.nested_floats = [(toFloat64(0.0),)];
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_float_equality WHERE data.nested_floats = [(toFloat64(0.0),)]
)
WHERE position(explain, '__text_index') > 0;

SELECT 'signed zero direct read plan';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_float_equality WHERE data.f64 = toFloat64(0.0)
)
WHERE position(explain, '__text_index') > 0;

DROP TABLE json_index_tokens_float_equality;

CREATE TABLE json_index_tokens_zero_limit
(
    data JSON,
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(0))
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_index_tokens_tiny_limit
(
    id UInt64,
    data JSON(s String),
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(1)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
INSERT INTO json_index_tokens_tiny_limit VALUES (1, '{"s":"value"}');
SELECT groupArray(id) FROM json_index_tokens_tiny_limit WHERE data.s = 'value';
SELECT groupArray(id) FROM json_index_tokens_tiny_limit WHERE data.s = 'value'
SETTINGS force_data_skipping_indices = 'json_tokens'; -- { serverError INDEX_NOT_USED }
CHECK TABLE json_index_tokens_tiny_limit SETTINGS check_query_single_value_result = 1;
DROP TABLE json_index_tokens_tiny_limit;

CREATE TABLE json_index_tokens_tiny_pattern_limit
(
    id UInt64,
    data JSON(s String),
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(8)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
INSERT INTO json_index_tokens_tiny_pattern_limit VALUES (1, '{"s":"long pattern value"}');
SELECT groupArray(id) FROM json_index_tokens_tiny_pattern_limit WHERE startsWith(data.s, 'long');
SELECT groupArray(id) FROM json_index_tokens_tiny_pattern_limit WHERE startsWith(data.s, 'long')
SETTINGS force_data_skipping_indices = 'json_tokens'; -- { serverError INDEX_NOT_USED }
CHECK TABLE json_index_tokens_tiny_pattern_limit SETTINGS check_query_single_value_result = 1;
DROP TABLE json_index_tokens_tiny_pattern_limit;
