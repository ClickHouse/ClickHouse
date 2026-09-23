-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- The text index is analyzed on the expression under lossless conversions of the haystack, e.g. `s` in
-- `hasToken(toNullable(s), 'Foo')`, and its preprocessor is applied to the needle. The row-level function has
-- to apply the preprocessor to the unwrapped haystack as well, so the result doesn't depend on whether the
-- index is read directly or the part has the index materialized.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_postprocessor;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

CREATE TABLE tab_postprocessor
(
    id UInt64,
    s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s), postprocessor = if(s = 'bar', '', s))
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

SYSTEM STOP MERGES tab;
SYSTEM STOP MERGES tab_postprocessor;

INSERT INTO tab VALUES (1, 'Foo bar'), (2, 'foo bar'), (3, 'baz');
INSERT INTO tab_postprocessor VALUES (1, 'Foo bar'), (2, 'foo bar'), (3, 'baz');

-- { echo }

SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s, 'Foo') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s, 'Foo') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(toNullable(s), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(toNullable(s), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(CAST(s, 'Nullable(String)'), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(CAST(s, 'Nullable(String)'), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(toLowCardinality(s), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(toLowCardinality(s), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(toNullable(s), 'Foo Qux') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(toNullable(s), 'Foo Qux') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(toNullable(s), 'Foo Bar') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(toNullable(s), 'Foo Bar') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT arraySort(groupArray(id)) FROM tab_postprocessor WHERE hasToken(toNullable(s), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_postprocessor WHERE hasToken(toNullable(s), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 0;

-- A part without the materialized index evaluates the row-level function.
INSERT INTO tab SETTINGS materialize_skip_indexes_on_insert = 0 VALUES (4, 'Foo qux'), (5, 'foo qux');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(toNullable(s), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(toNullable(s), 'Foo') SETTINGS query_plan_direct_read_from_text_index = 0;

-- { echoOff }

DROP TABLE tab;
DROP TABLE tab_postprocessor;
