-- Tags: no-parallel-replicas

-- Tests that the bloom-filter text indexes (ngrambf_v1, tokenbf_v1, sparse_grams) are used when the
-- predicate uses `LIKE pattern ESCAPE 'c'` or `NOT LIKE pattern ESCAPE 'c'`. `ILIKE` is not covered
-- because these index types do not support case-insensitive LIKE at all.

SET enable_analyzer = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 128, index_granularity_bytes = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO tab SELECT number, 'Hello ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello World, ClickHouse is fast!' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo xClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHousez rocks' FROM numbers(1024);
INSERT INTO tab SELECT number, 'literal 50%off token' FROM numbers(1024);

SELECT 'Results are the same with and without an ESCAPE clause when the escape character is not used in the pattern';

SELECT count() FROM tab WHERE message LIKE '%World%';
SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|';
SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '#';

SELECT count() FROM tab WHERE message NOT LIKE '%World%';
SELECT count() FROM tab WHERE message NOT LIKE '%World%' ESCAPE '|';

SELECT 'ESCAPE used to match a literal LIKE wildcard';

SELECT count() FROM tab WHERE message LIKE '%50#%off%' ESCAPE '#';
SELECT count() FROM tab WHERE message LIKE '%fast|%' ESCAPE '|';

SELECT 'force_data_skipping_indices accepts the ESCAPE, functional and NOT LIKE forms, just like the plain 2-argument LIKE';

SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE like(message, '%World%', '|') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE message NOT LIKE '%World%' ESCAPE '|' SETTINGS force_data_skipping_indices = 'idx';

SELECT 'Index analysis with LIKE ESCAPE narrows to the matching part (8 of 40 granules), same as the plain 2-argument LIKE';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%World%'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT 'Index analysis with a consumed escape character also narrows to 8 of 40 granules';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%50#%off%' ESCAPE '#'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT 'Functional 3-argument form like(haystack, needle, escape) is equivalent to the operator form and also narrows to 8 of 40 granules';

SELECT count() FROM tab WHERE like(message, '%World%', '|');

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE like(message, '%World%', '|')
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'A tokenbf_v1 text index also uses the index with LIKE ESCAPE, with the same plan as the plain LIKE (16 of 32 granules)';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE tokenbf_v1(512, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 128, index_granularity_bytes = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO tab SELECT number, 'Hello ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello World ClickHouse is fast' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo xClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHousez rocks' FROM numbers(1024);

SELECT count() FROM tab WHERE message LIKE 'Hello World%';
SELECT count() FROM tab WHERE message LIKE 'Hello World%' ESCAPE '|';

SELECT count() FROM tab WHERE message LIKE 'Hello World%' ESCAPE '|' SETTINGS force_data_skipping_indices = 'idx';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE 'Hello World%'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE 'Hello World%' ESCAPE '|'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'A sparse_grams text index also uses the index with LIKE ESCAPE (8 of 24 granules)';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE sparse_grams(3, 100, 512, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 128, index_granularity_bytes = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO tab SELECT number, 'Hello ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello World ClickHouse fast' FROM numbers(1024);
INSERT INTO tab SELECT number, 'literal 50%off token' FROM numbers(1024);

SELECT count() FROM tab WHERE message LIKE '%World%';
SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|';

SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|' SETTINGS force_data_skipping_indices = 'idx';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT 'Index analysis with a consumed escape character also narrows to 8 of 24 granules';

SELECT count() FROM tab WHERE message LIKE '%50#%off%' ESCAPE '#';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%50#%off%' ESCAPE '#'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'A non-ASCII ESCAPE byte is rejected, also by EXPLAIN';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE tokenbf_v1(512, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'foo bar'), (2, 'baz qux'), (3, 'nothing here');

SELECT count() FROM tab WHERE like(message, '%bar%', unhex('FF')); -- { serverError BAD_ARGUMENTS }
EXPLAIN indexes = 1 SELECT count() FROM tab WHERE like(message, '%bar%', unhex('FF')); -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;

SELECT 'A pattern containing a literal backslash matches, and no granule is skipped';

-- `\b` is not an escape sequence, so the pattern matches a literal backslash followed by `b`.
-- ('foo a\\b bar' below is the string with one backslash before b.)

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE tokenbf_v1(512, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'foo a\\b bar'), (2, 'foo ab bar'), (3, 'nothing here');

SELECT 'Only the row containing the literal backslash is returned';

SELECT id FROM tab WHERE msg LIKE '% a\\b %' ESCAPE '\\' ORDER BY id;

SELECT 'All 3 granules are scanned';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE msg LIKE '% a\\b %' ESCAPE '\\'
) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'A pattern ending in a lone escape character is rejected, also by EXPLAIN';

-- ('abc\\' below is a, b, c, backslash; ESCAPE '\\' is a single backslash.)

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE tokenbf_v1(512, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'zzz none'), (2, 'qqq nothing'), (3, 'more here');

EXPLAIN indexes = 1 SELECT count() FROM tab WHERE msg LIKE 'abc\\' ESCAPE '\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }
SELECT count() FROM tab WHERE msg LIKE 'abc\\' ESCAPE '\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

DROP TABLE tab;
