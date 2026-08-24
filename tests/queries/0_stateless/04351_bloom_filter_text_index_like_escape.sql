-- Tags: no-parallel-replicas

-- Tests that the bloom-filter text indexes (ngrambf_v1, tokenbf_v1, sparse_grams) are used when the
-- predicate uses `LIKE pattern ESCAPE 'c'` or `NOT LIKE pattern ESCAPE 'c'`. The escape character is
-- folded into the pattern (rewritten to standard backslash escapes) before the index dispatches it
-- through the existing 2-argument handler. `ilike` is not covered because these index types do not
-- support case-insensitive LIKE.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
-- max_bytes_to_merge_at_max_space_in_pool = 1: the granule counts below hold only while each
-- message value stays in its own part.
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

-- The escape character is actually consumed here: '50#%off' with ESCAPE '#' matches the literal
-- substring '50%off'. The folded pattern still tokenizes and the index is used (see the plan below).
SELECT count() FROM tab WHERE message LIKE '%50#%off%' ESCAPE '#';
-- No row contains a literal 'fast%', so this is empty.
SELECT count() FROM tab WHERE message LIKE '%fast|%' ESCAPE '|';

SELECT 'The bloom-filter text index prunes with LIKE ESCAPE just like the plain 2-argument LIKE (force_data_skipping_indices accepts the folded query, including the negated and functional forms)';

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

SELECT 'A sparse_grams text index also uses the index with LIKE ESCAPE (8 of 16 granules)';

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

SELECT count() FROM tab WHERE message LIKE '%World%';
SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|';

SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|' SETTINGS force_data_skipping_indices = 'idx';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'A non-ASCII ESCAPE byte is rejected at planning time so a granule cannot be skipped for a query that must raise';

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

SELECT 'An unknown backslash escape in the folded 3-argument form is not pruned by the bloom-filter text index';

-- The row contains the literal token "a\b" (a, backslash, b). `splitByNonAlpha` (the tokenbf_v1
-- tokenizer) indexes it as the tokens "a" and "b", but `nextInStringLike` on the pattern drops the
-- backslash and asks for the single token "ab", which is absent, so the granule would be wrongly
-- pruned. The analyzer declines the condition for unknown backslash escapes and falls back to
-- row-level. ('foo a\\b bar' is the literal string with one backslash before b.)

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

SELECT 'Correctness check (3-argument ESCAPE form): the row containing the literal backslash is returned';

SELECT id FROM tab WHERE msg LIKE '% a\\b %' ESCAPE '\\' ORDER BY id;

SELECT 'Index analysis declines the 3-argument condition: all 3 granules are scanned';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE msg LIKE '% a\\b %' ESCAPE '\\'
) WHERE explain LIKE '%Granules:%';

DROP TABLE tab;

SELECT 'A trailing escape character in a 3-argument LIKE ESCAPE raises at index-analysis time (the fold rejects it, so a granule cannot be skipped for a query that must raise)';

-- With `ESCAPE '\'` the pattern `abc\` ends in a lone escape character, which is an invalid escape
-- sequence. The 3-argument fold runs `likePatternWithCustomEscapeToLikePattern` during index analysis
-- and raises CANNOT_PARSE_ESCAPE_SEQUENCE, exactly as row-level `LIKE` does. This prevents the index
-- from silently pruning every granule and turning a query that must raise into an empty result.
-- ('abc\\' is a, b, c, backslash; ESCAPE '\\' is a single backslash.)

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

SELECT 'Index analysis raises instead of pruning';

EXPLAIN indexes = 1 SELECT count() FROM tab WHERE msg LIKE 'abc\\' ESCAPE '\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

SELECT 'Correctness check: the query raises instead of silently returning an empty result';

SELECT count() FROM tab WHERE msg LIKE 'abc\\' ESCAPE '\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

DROP TABLE tab;
