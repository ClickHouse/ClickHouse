-- Tests that a text index whose expression contains an empty string comparison (e.g.
-- `arrayFilter(s -> s != '', ...)`) is used regardless of `optimize_empty_string_comparisons`,
-- which rewrites `s != ''` to `notEmpty(s)` on the query side (issue #111788).

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET query_plan_direct_read_from_text_index = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab;

SELECT 'Index on an alias column filtering out empty strings (issue #111788)';

CREATE TABLE tab
(
    time DateTime,
    event String,
    x_event Array(String) ALIAS arrayFilter(s -> s != '', JSONExtractKeys(event)),
    INDEX fts_x_event x_event TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY time
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (1, '{"xoo":1,"":1}'), (2, '{"foo":1}'), (3, '{"bar":1}'), (4, '{"baz":1}'), (5, '{"qux":1}'), (6, '{"waldo":1}');

SELECT '-- Results with and without the optimization';
SELECT count() FROM tab WHERE hasAllTokens(x_event, ['xoo']) SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE hasAllTokens(x_event, ['xoo']) SETTINGS optimize_empty_string_comparisons = 0;

SELECT '-- The index must be used with the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(x_event, ['xoo'])
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

SELECT '-- The index must be used without the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(x_event, ['xoo'])
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'Index on an alias column written with notEmpty';

CREATE TABLE tab
(
    time DateTime,
    event String,
    x_event Array(String) ALIAS arrayFilter(s -> notEmpty(s), JSONExtractKeys(event)),
    INDEX fts_x_event x_event TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY time
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (1, '{"xoo":1,"":1}'), (2, '{"foo":1}'), (3, '{"bar":1}'), (4, '{"baz":1}'), (5, '{"qux":1}'), (6, '{"waldo":1}');

SELECT '-- Results with and without the optimization';
SELECT count() FROM tab WHERE hasAllTokens(x_event, ['xoo']) SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE hasAllTokens(x_event, ['xoo']) SETTINGS optimize_empty_string_comparisons = 0;

SELECT '-- The index must be used with the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(x_event, ['xoo'])
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'Index on an expression with equals to empty string';

CREATE TABLE tab
(
    id UInt64,
    arr Array(String),
    INDEX idx arrayMap(s -> if(s = '', 'MISSING', s), arr) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (1, ['xoo', '']), (2, ['foo']), (3, ['bar']), (4, ['baz']), (5, ['qux']), (6, ['waldo']);

SELECT '-- Results with and without the optimization';
SELECT count() FROM tab WHERE hasAllTokens(arrayMap(s -> if(s = '', 'MISSING', s), arr), ['MISSING']) SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE hasAllTokens(arrayMap(s -> if(s = '', 'MISSING', s), arr), ['MISSING']) SETTINGS optimize_empty_string_comparisons = 0;

SELECT '-- The index must be used with the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(arrayMap(s -> if(s = '', 'MISSING', s), arr), ['MISSING'])
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

SELECT '-- The index must be used without the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE hasAllTokens(arrayMap(s -> if(s = '', 'MISSING', s), arr), ['MISSING'])
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'Index on mapKeys of an alias column filtering out empty keys';

CREATE TABLE tab
(
    id UInt64,
    m Map(String, String),
    filtered_m Map(String, String) ALIAS mapFilter((k, v) -> k != '', m),
    INDEX idx mapKeys(filtered_m) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (1, {'xoo':'a', '':'b'}), (2, {'foo':'a'}), (3, {'bar':'a'}), (4, {'baz':'a'}), (5, {'qux':'a'}), (6, {'waldo':'a'});

SELECT '-- Results with and without the optimization';
SELECT count() FROM tab WHERE mapContainsKey(filtered_m, 'xoo') SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE mapContainsKey(filtered_m, 'xoo') SETTINGS optimize_empty_string_comparisons = 0;

SELECT '-- The index must be used with the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE mapContainsKey(filtered_m, 'xoo')
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

SELECT '-- The index must be used without the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE mapContainsKey(filtered_m, 'xoo')
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'Index on mapValues of an alias column filtering out empty values';

CREATE TABLE tab
(
    id UInt64,
    m Map(String, String),
    filtered_m Map(String, String) ALIAS mapFilter((k, v) -> v != '', m),
    INDEX idx mapValues(filtered_m) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (1, {'k':'xoo', 'e':''}), (2, {'k':'foo'}), (3, {'k':'bar'}), (4, {'k':'baz'}), (5, {'k':'qux'}), (6, {'k':'waldo'});

SELECT '-- Results with and without the optimization';
SELECT count() FROM tab WHERE mapContainsValue(filtered_m, 'xoo') SETTINGS optimize_empty_string_comparisons = 1;
SELECT count() FROM tab WHERE mapContainsValue(filtered_m, 'xoo') SETTINGS optimize_empty_string_comparisons = 0;

SELECT '-- The index must be used with the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE mapContainsValue(filtered_m, 'xoo')
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

SELECT '-- The index must be used without the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE mapContainsValue(filtered_m, 'xoo')
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

SELECT '-- The index must be used for map element access with the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE filtered_m['k'] = 'xoo'
    SETTINGS optimize_empty_string_comparisons = 1
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

SELECT '-- The index must be used for map element access without the optimization';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE filtered_m['k'] = 'xoo'
    SETTINGS optimize_empty_string_comparisons = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;
