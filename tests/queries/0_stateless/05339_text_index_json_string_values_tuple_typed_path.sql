-- A String leaf inside a typed Tuple is not walked by `jsonStringValues`. Treating
-- `hasToken(json.a.b, ...)` as Exact would prune every granule and drop matching rows.
-- Nested `JSON` typed prefixes are walked and must stay Exact.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET make_distributed_plan = 0;

DROP TABLE IF EXISTS tab_tuple;
CREATE TABLE tab_tuple
(
    id UInt32,
    json JSON(a Tuple(b String)),
    INDEX idx json TYPE text(tokenizer = 'jsonStringValues') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab_tuple VALUES (0, '{"a": {"b": "hello"}}');

SELECT '-- Tuple-nested String is found without the index';
SELECT id FROM tab_tuple WHERE hasToken(json.a.b, 'hello') SETTINGS use_skip_indexes = 0;

SELECT '-- the index is not used for a Tuple-nested String';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT id FROM tab_tuple WHERE hasToken(json.a.b, 'hello'))
WHERE explain LIKE '%Name: idx%';
SELECT id FROM tab_tuple WHERE hasToken(json.a.b, 'hello') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT '-- explicit .:String under a Tuple is also refused';
SELECT id FROM tab_tuple WHERE hasToken(json.a.b.:`String`, 'hello') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab_tuple;

SELECT '-- nested JSON typed path is still Exact';
DROP TABLE IF EXISTS tab_nested;
CREATE TABLE tab_nested
(
    id UInt32,
    json JSON(a JSON(b String)),
    INDEX idx json TYPE text(tokenizer = 'jsonStringValues') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab_nested VALUES (0, '{"a": {"b": "hello"}}');

SELECT 'idx', id FROM tab_nested WHERE hasToken(json.a.b, 'hello') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_nested WHERE hasToken(json.a.b, 'hello') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_nested;
