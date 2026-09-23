SET allow_experimental_full_text_index = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_json_all_values_wrapped_types;

CREATE TABLE t_json_all_values_wrapped_types
(
    data JSON(
        ip Nullable(IPv4),
        tag LowCardinality(Nullable(String)),
        tags Array(Nullable(String))),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_json_all_values_wrapped_types
SELECT multiIf(
    number < 4, '{"ip":"1.2.3.4","tag":"needle","tags":["bug",null]}',
    number < 8, '{"ip":"8.8.8.8","tag":"other","tags":["feature"]}',
    '{}')
FROM numbers(12);

-- Wrapper-only differences preserve the text representation stored by `JSONAllValues`.
SELECT count() FROM t_json_all_values_wrapped_types WHERE data.ip = toIPv4('1.2.3.4');
SELECT count() FROM t_json_all_values_wrapped_types WHERE data.tag = 'needle';
SELECT count() FROM t_json_all_values_wrapped_types WHERE has(data.tags, 'bug');

-- Compare with row-level evaluation without the `text` index.
SELECT count() FROM t_json_all_values_wrapped_types WHERE data.ip = toIPv4('1.2.3.4') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_wrapped_types WHERE data.tag = 'needle' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_wrapped_types WHERE has(data.tags, 'bug') SETTINGS use_skip_indexes = 0;

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_all_values_wrapped_types WHERE data.ip = toIPv4('1.2.3.4')
)
WHERE explain LIKE '%Granules: 1/3%';

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_all_values_wrapped_types WHERE data.tag = 'needle'
)
WHERE explain LIKE '%Granules: 1/3%';

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_all_values_wrapped_types WHERE has(data.tags, 'bug')
)
WHERE explain LIKE '%Granules: 1/3%';

DROP TABLE t_json_all_values_wrapped_types;

CREATE TABLE t_json_all_values_has_array_tokenizer
(
    data JSON(tags Array(String)),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_json_all_values_has_array_tokenizer
SELECT if(number < 4, '{"tags":["bug","x"]}', '{"tags":["other"]}')
FROM numbers(8);

SELECT count() FROM t_json_all_values_has_array_tokenizer WHERE has(data.tags, 'bug');
SELECT count() FROM t_json_all_values_has_array_tokenizer WHERE has(data.tags, 'bug') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_has_array_tokenizer WHERE hasAnyTokens(data.tags, ['bug']);
SELECT count() FROM t_json_all_values_has_array_tokenizer WHERE hasAnyTokens(data.tags, ['bug']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_has_array_tokenizer WHERE hasAllTokens(data.tags, ['bug']);
SELECT count() FROM t_json_all_values_has_array_tokenizer WHERE hasAllTokens(data.tags, ['bug']) SETTINGS use_skip_indexes = 0;

DROP TABLE t_json_all_values_has_array_tokenizer;
