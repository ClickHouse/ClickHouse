SET allow_experimental_full_text_index = 1;
SET explain_query_plan_default = 'legacy';
SET input_format_try_infer_datetimes = 1;
SET input_format_try_infer_datetimes_only_datetime64 = 0;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_json_all_values_representation_types;

CREATE TABLE t_json_all_values_representation_types
(
    data JSON(
        explicit_dt DateTime('Europe/Moscow'),
        implicit_dt DateTime,
        flag Bool,
        id UInt64),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_json_all_values_representation_types
SELECT '{"explicit_dt":"2020-01-01 03:00:00","implicit_dt":"2030-02-02 00:00:00","dynamic_dt":"2040-03-03 00:00:00","flag":true,"id":42,"labels":[{"name":"bug"}]}'
FROM numbers(4);

INSERT INTO t_json_all_values_representation_types
SELECT '{"explicit_dt":"2021-06-06 06:00:00","implicit_dt":"2031-07-07 00:00:00","dynamic_dt":"2041-08-08 00:00:00","flag":false,"id":7,"labels":[{"name":"feature"}]}'
FROM numbers(4);

SET session_timezone = 'Europe/Moscow';

-- Stable typed paths serialize exactly converted constants using the path type.
SELECT count() FROM t_json_all_values_representation_types WHERE data.explicit_dt = toDateTime('2020-01-01 00:00:00', 'UTC');
SELECT count() FROM t_json_all_values_representation_types WHERE data.flag = 1;

-- Implicit time zones can change the indexed representation between insertion and query execution.
SELECT count() FROM t_json_all_values_representation_types WHERE data.implicit_dt = toDateTime('2030-02-02 03:00:00');

-- The runtime type of a dynamic path cannot prove representation compatibility.
SELECT count() FROM t_json_all_values_representation_types WHERE data.dynamic_dt = '2040-03-03 03:00:00';
SELECT count() FROM t_json_all_values_representation_types WHERE data.dynamic_dt = toDateTime('2040-03-03 03:00:00', 'Europe/Moscow');
SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_all_values_representation_types WHERE data.dynamic_dt::String = '2040-03-03 03:00:00'
)
WHERE explain LIKE '%idx_values%';
SELECT count() FROM t_json_all_values_representation_types WHERE data.labels[].name::String = '[''bug'']';
SELECT count() FROM t_json_all_values_representation_types WHERE data.labels[].name::String IN (SELECT '[''bug'']');

-- Compare with row-level evaluation without the `text` index.
SELECT count() FROM t_json_all_values_representation_types WHERE data.explicit_dt = toDateTime('2020-01-01 00:00:00', 'UTC') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_representation_types WHERE data.flag = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_representation_types WHERE data.implicit_dt = toDateTime('2030-02-02 03:00:00') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_representation_types WHERE data.dynamic_dt = '2040-03-03 03:00:00' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_representation_types WHERE data.dynamic_dt = toDateTime('2040-03-03 03:00:00', 'Europe/Moscow') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_representation_types WHERE data.labels[].name::String = '[''bug'']' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_representation_types WHERE data.labels[].name::String IN (SELECT '[''bug'']') SETTINGS use_skip_indexes = 0;

-- Canonical string sets still use the index, while alternative spellings keep row-level semantics.
SELECT count() FROM t_json_all_values_representation_types WHERE data.id IN (SELECT '42');
SELECT count() FROM t_json_all_values_representation_types WHERE data.id IN (SELECT '042');
SELECT count() FROM t_json_all_values_representation_types WHERE data.id IN (SELECT '42') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_representation_types WHERE data.id IN (SELECT '042') SETTINGS use_skip_indexes = 0;

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_all_values_representation_types WHERE data.flag = 1
)
WHERE explain LIKE '%Granules: 1/2%';

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_all_values_representation_types WHERE data.id IN (SELECT '42')
)
WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_json_all_values_representation_types;

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_json_all_values_array_dynamic;

CREATE TABLE t_json_all_values_array_dynamic
(
    data JSON(tags Array(Dynamic)),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_json_all_values_array_dynamic
SELECT if(
    number < 4,
    '{"tags":["2040-03-03 00:00:00"]}',
    '{"tags":["2041-04-04 00:00:00"]}')
FROM numbers(8);

SET session_timezone = 'Europe/Moscow';

-- Keep the needle `Dynamic` to exercise the unsafe `Array(Dynamic)` index path.
SELECT count() FROM t_json_all_values_array_dynamic
WHERE has(data.tags, CAST(toDateTime('2040-03-03 03:00:00') AS Dynamic))
SETTINGS use_skip_indexes = 1;

SELECT count() FROM t_json_all_values_array_dynamic
WHERE has(data.tags, CAST(toDateTime('2040-03-03 03:00:00') AS Dynamic))
SETTINGS use_skip_indexes = 0;

DROP TABLE t_json_all_values_array_dynamic;
