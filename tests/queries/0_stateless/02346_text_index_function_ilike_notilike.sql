-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET use_query_condition_cache = 0;

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

SET use_text_index_like_optimization = 0;

SELECT '-- without optimization';

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%' AND message ILIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';

SELECT '-- with optimization';

SET use_text_index_like_optimization = 1;

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%' AND message ILIKE '%abc%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';

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

SET use_text_index_like_optimization = 0;

SELECT '-- without optimization';

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';

SELECT '-- with optimization';

SET use_text_index_like_optimization = 1;

SELECT groupArray(id) FROM tab WHERE message ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOO%';
SELECT groupArray(id) FROM tab WHERE message NOT ILIKE '%foo%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%abc%' AND message NOT ILIKE '%foo%';

DROP TABLE tab;

SELECT 'Text index analysis';

SET use_text_index_like_optimization = 1;

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
