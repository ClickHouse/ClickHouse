-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111788
-- A text index is matched to the query expression by column name. Queries are analyzed with
-- `optimize_empty_string_comparisons` (enabled by default), which rewrites `s = ''` to `empty(s)`,
-- while the index expression is not, so an index whose expression contains an empty string
-- comparison was never used. The index must be used with and without the optimization.

SET enable_analyzer = 1; -- `optimize_empty_string_comparisons` is an analyzer pass
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET query_plan_direct_read_from_text_index = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab;

SELECT 'Index on an expression comparing to an empty string';

CREATE TABLE tab
(
    s String,
    INDEX idx if(s = '', 'abc', 'def') TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (''), ('x'), ('y'), ('z');

SELECT count() FROM tab WHERE hasAllTokens(if(s = '', 'abc', 'def'), ['abc']) SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE hasAllTokens(if(s = '', 'abc', 'def'), ['abc']) SETTINGS optimize_empty_string_comparisons = 0;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(if(s = '', 'abc', 'def'), ['abc'])
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(if(s = '', 'abc', 'def'), ['abc'])
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'Index on an expression already written with empty';

CREATE TABLE tab
(
    s String,
    INDEX idx if(empty(s), 'abc', 'def') TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (''), ('x'), ('y'), ('z');

SELECT count() FROM tab WHERE hasAllTokens(if(empty(s), 'abc', 'def'), ['abc']) SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE hasAllTokens(if(empty(s), 'abc', 'def'), ['abc']) SETTINGS optimize_empty_string_comparisons = 0;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(if(empty(s), 'abc', 'def'), ['abc'])
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'Index on an alias column comparing to an empty string';

CREATE TABLE tab
(
    s String,
    a String ALIAS if(s = '', 'abc', 'def'),
    INDEX idx a TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (''), ('x'), ('y'), ('z');

SELECT count() FROM tab WHERE hasAllTokens(a, ['abc']) SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE hasAllTokens(a, ['abc']) SETTINGS optimize_empty_string_comparisons = 0;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(a, ['abc'])
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(a, ['abc'])
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'Index on mapKeys of an alias column comparing to an empty string';

CREATE TABLE tab
(
    m Map(String, String),
    fm Map(String, String) ALIAS mapFilter((k, v) -> k != '', m),
    INDEX idx mapKeys(fm) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES ({'abc':'1', '':'2'}), ({'def':'1'}), ({'ghi':'1'}), ({'jkl':'1'});

SELECT count() FROM tab WHERE mapContainsKey(fm, 'abc') SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE mapContainsKey(fm, 'abc') SETTINGS optimize_empty_string_comparisons = 0;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE mapContainsKey(fm, 'abc')
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE mapContainsKey(fm, 'abc')
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'Index on mapValues of an alias column comparing to an empty string';

CREATE TABLE tab
(
    m Map(String, String),
    fm Map(String, String) ALIAS mapFilter((k, v) -> v != '', m),
    INDEX idx mapValues(fm) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES ({'k':'abc', 'e':''}), ({'k':'def'}), ({'k':'ghi'}), ({'k':'jkl'});

SELECT count() FROM tab WHERE mapContainsValue(fm, 'abc') SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE mapContainsValue(fm, 'abc') SETTINGS optimize_empty_string_comparisons = 0;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE mapContainsValue(fm, 'abc')
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE mapContainsValue(fm, 'abc')
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT 'The same index for map element access';

SELECT count() FROM tab WHERE fm['k'] = 'abc' SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE fm['k'] = 'abc' SETTINGS optimize_empty_string_comparisons = 0;

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE fm['k'] = 'abc'
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE fm['k'] = 'abc'
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;
