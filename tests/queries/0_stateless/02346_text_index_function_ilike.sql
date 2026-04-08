-- Tags: no-parallel-replicas

-- Tests if a text index ILIKE evaluation by scanning the inverted index dictionary is properly applied.

SET enable_analyzer = 1;

SELECT 'Test results are same with/without the like optimization';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, message) VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar'),
    (3, 'abc baz foo'),
    (4, 'abc baz bar'),
    (5, 'xyz');

SET use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT '-- without optimization';

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%' AND message ILIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message ILIKE '%ABC%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message ILIKE '%DEF%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message NOT ILIKE '%DEF%';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['foo', 'bar']) AND message ILIKE '%ABC%';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo', 'abc']) AND message ILIKE '%DEF%';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo', 'abc']) AND message NOT ILIKE '%BAZ%';

SELECT '-- with optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%' AND message ILIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message ILIKE '%ABC%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message ILIKE '%DEF%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message NOT ILIKE '%DEF%';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['foo', 'bar']) AND message ILIKE '%ABC%';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo', 'abc']) AND message ILIKE '%DEF%';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo', 'abc']) AND message NOT ILIKE '%BAZ%';

DROP TABLE tab;

SELECT 'Test results are same with/without the like optimization with preprocessor';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(message))
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, message) VALUES
    (1, 'ABC DEF FOO'),
    (2, 'ABC DEF BAR'),
    (3, 'abc BAZ foo'),
    (4, 'abc baz bar'),
    (5, 'xyz');

SET use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT '-- without optimization';

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';

SELECT '-- with optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';

DROP TABLE tab;

SELECT 'Text index analysis';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT number, 'Hello ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello World, ClickHouse is fast!' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo xClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHousez rocks' FROM numbers(1024);

SELECT '-- Text index for ILIKE function should choose none for non-existent token';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message ILIKE '%random%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Text index for ILIKE function should choose 1 part and 1024 granules (lowercase pattern matches uppercase token)';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message ILIKE '%world%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Text index for ILIKE function should choose same 1 part and 1024 granules (uppercase pattern, case-insensitive)';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message ILIKE '%WORLD%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Text index for ILIKE function should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message ILIKE '%hello%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Text index for ILIKE function should choose all 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message ILIKE '%clickhouse%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;

SELECT 'Tests fallback when threshold (text_index_like_max_postings_to_read) is exceeded for reading large postings';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, concat(char(97 + (number % 26)), char(97 + intDiv(number, 26) % 26)) FROM numbers(100000);

SELECT count() FROM tab WHERE message ILIKE '%A%';
SELECT count() FROM tab WHERE message ILIKE '%A%' SETTINGS use_skip_indexes = 0;

SELECT count() FROM tab WHERE message NOT ILIKE '%A%';
SELECT count() FROM tab WHERE message NOT ILIKE '%A%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'Test results are same with/without the optimization with expression index';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx lower(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, message) VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar'),
    (3, 'abc BAZ FOO'),
    (4, 'abc baz bar'),
    (5, 'xyz');

SELECT '-- without optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%' AND message ILIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';

SELECT '-- with optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%' AND message ILIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';

DROP TABLE tab;

SELECT 'Tests fallback when threshold (text_index_like_min_pattern_length) is exceeded for expression index';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx lower(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, concat(char(97 + (number % 26)), char(97 + intDiv(number, 26) % 26)) FROM numbers(100000);

SELECT count() FROM tab WHERE message ILIKE '%A%';
SELECT count() FROM tab WHERE message ILIKE '%A%' SETTINGS use_skip_indexes = 0;

SELECT count() FROM tab WHERE message NOT ILIKE '%A%';
SELECT count() FROM tab WHERE message NOT ILIKE '%A%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'Test ILIKE optimization is disabled for nested preprocessors (lower(reverse(col)))';

-- lower(reverse(message)) is not pure case folding.
-- For 'FOO BAR': reverse → 'RAB OOF', lower → 'rab oof', tokens: ['rab', 'oof'].
-- ILIKE '%foo%' on the raw message is TRUE, but 'foo' is absent from the dictionary.
-- Without the isCaseFolding() guard the optimization would skip row 1 (wrong result).
DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(reverse(message)))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab(id, message) VALUES (1, 'FOO BAR'), (2, 'XYZ');

SET use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT '-- without optimization';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT '-- with optimization (must produce same results)';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';

DROP TABLE tab;

SELECT 'Test ILIKE optimization is disabled for non-case-folding preprocessor (reverse(col))';

-- reverse(message) is not case folding at all.
-- For 'foo bar': reverse → 'rab oof', tokens: ['rab', 'oof'].
-- Without the guard the optimization would look for 'foo' in the dictionary and skip row 1.
DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = reverse(message))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab(id, message) VALUES (1, 'foo bar'), (2, 'xyz');

SET use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT '-- without optimization';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT '-- with optimization (must produce same results)';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';

DROP TABLE tab;
