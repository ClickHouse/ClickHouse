-- Tags: no-parallel-replicas

-- Tests if a text index LIKE evaluation by scanning the inverted index dictionary is properly applied.

SET enable_analyzer = 1;

SELECT 'Test results are same with/without the optimization';

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

SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%xyz%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%xyz%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%' AND message LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%' AND message LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%abc%' AND message NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message LIKE '%def%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message NOT LIKE '%def%';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['foo', 'bar']) AND message LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo', 'abc']) AND message LIKE '%def%';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo', 'abc']) AND message NOT LIKE '%baz%';

SELECT '-- with optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%xyz%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%xyz%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%' AND message LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%' AND message LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%abc%' AND message NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message LIKE '%def%';
SELECT groupArray(id) FROM tab WHERE hasToken(message, 'foo') AND message NOT LIKE '%def%';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['foo', 'bar']) AND message LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo', 'abc']) AND message LIKE '%def%';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo', 'abc']) AND message NOT LIKE '%baz%';

DROP TABLE tab;

SELECT 'Test results are same with/without the optimization with preprocessor';

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

SELECT '-- without optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%' AND message LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%' AND message LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%abc%' AND message NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%foo%' AND message NOT LIKE '%bar%';

SELECT '-- with optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%' AND message LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foo%' AND message LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%abc%' AND message NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%foo%' AND message NOT LIKE '%bar%';

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

SELECT '-- Text index for LIKE function should choose none for non-existent token';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message LIKE '%random%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Text index for LIKE function should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message LIKE '%World%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Text index for LIKE function should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message LIKE '%Hello%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Text index for LIKE function should choose all 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message LIKE '%ClickHouse%'
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

SELECT count() FROM tab WHERE message LIKE '%a%';
SELECT count() FROM tab WHERE message LIKE '%a%' SETTINGS use_skip_indexes = 0;

SELECT count() FROM tab WHERE message NOT LIKE '%a%';
SELECT count() FROM tab WHERE message NOT LIKE '%a%' SETTINGS use_skip_indexes = 0;

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

SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%baz%';
SELECT groupArray(id) FROM tab WHERE lower(message) NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%foo%' AND lower(message) LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%abc%' AND lower(message) NOT LIKE '%foo%';

SELECT '-- with optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%bar%';
SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%baz%';
SELECT groupArray(id) FROM tab WHERE lower(message) NOT LIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%foo%' AND lower(message) LIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE lower(message) LIKE '%abc%' AND lower(message) NOT LIKE '%foo%';

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

SELECT count() FROM tab WHERE lower(message) LIKE '%a%';
SELECT count() FROM tab WHERE lower(message) LIKE '%a%' SETTINGS use_skip_indexes = 0;

SELECT count() FROM tab WHERE lower(message) NOT LIKE '%a%';
SELECT count() FROM tab WHERE lower(message) NOT LIKE '%a%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
