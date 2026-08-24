-- Tags: no-parallel-replicas

-- Tests that the text index is still used when the predicate uses
-- `LIKE pattern ESCAPE 'c'` or `ILIKE pattern ESCAPE 'c'`. The escape
-- character is folded into the pattern (rewritten to standard backslash
-- escapes) before the index dispatches it through the existing 2-argument
-- handlers.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
-- A moderate index_granularity keeps the text index small (few granules) so the
-- flaky check stays well under its per-run time limit; the five single-value parts
-- still demonstrate that a `LIKE ... ESCAPE` predicate prunes to the matching part.
-- The granule counts below hold only for this exact layout: index_granularity_bytes = 0 keeps
-- granularity row-based, and max_bytes_to_merge_at_max_space_in_pool = 1 keeps the five parts separate.
SETTINGS index_granularity = 128, index_granularity_bytes = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO tab SELECT number, 'Hello ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello World, ClickHouse is fast!' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo xClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHousez rocks' FROM numbers(1024);
INSERT INTO tab SELECT number, 'literal 50%off token' FROM numbers(1024);

SELECT 'Results are the same with and without an ESCAPE clause when the escape character is not used in the pattern';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT count() FROM tab WHERE message LIKE '%World%';
SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|';
SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '#';

SELECT count() FROM tab WHERE message ILIKE '%world%';
SELECT count() FROM tab WHERE message ILIKE '%world%' ESCAPE '|';

SELECT count() FROM tab WHERE message NOT LIKE '%World%';
SELECT count() FROM tab WHERE message NOT LIKE '%World%' ESCAPE '|';

SELECT 'ESCAPE used to match a literal LIKE wildcard returns the expected zero rows';

SELECT count() FROM tab WHERE message LIKE '%fast|%' ESCAPE '|';
SELECT count() FROM tab WHERE message LIKE '%fast#%' ESCAPE '#';

SELECT 'A consumed escape character is folded before matching: the escaped % is a literal, so the same pattern read without the ESCAPE clause matches nothing';

SELECT count() FROM tab WHERE message LIKE '%literal 50#%off token%' ESCAPE '#';
SELECT count() FROM tab WHERE message LIKE '%literal 50#%off token%';
SELECT count() FROM tab WHERE message LIKE '%literal 50\\%off token%';

SELECT 'The index still prunes for that folded pattern, to the same 8 of 40 granules as the equivalent standard-escape pattern';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%literal 50#%off token%' ESCAPE '#'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%literal 50\\%off token%'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT 'Text index analysis with LIKE ESCAPE: index narrows to 1 part / 8 granules out of 5 parts / 40 granules';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%World%' ESCAPE '|'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 3, 4;

SELECT 'Text index analysis with LIKE ESCAPE on a non-existent token: 0 parts and 0 granules selected';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%missing%' ESCAPE '|'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 3, 4;

SELECT 'Text index analysis with ILIKE ESCAPE: index narrows correctly';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message ILIKE '%world%' ESCAPE '|'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 3, 4;

SELECT 'Functional 3-argument form like(haystack, needle, escape) is equivalent to the operator form';

SELECT count() FROM tab WHERE like(message, '%World%', '|');
SELECT count() FROM tab WHERE ilike(message, '%world%', '|');

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE like(message, '%World%', '|')
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 3, 4;

DROP TABLE tab;

SELECT 'Text index with array tokenizer also uses the index with LIKE ESCAPE';

CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 128, index_granularity_bytes = 0, max_bytes_to_merge_at_max_space_in_pool = 1;  -- see the note above

INSERT INTO tab SELECT number, 'ClickHouseServer' FROM numbers(1024);
INSERT INTO tab SELECT number, 'clickhouseclient' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHouseCloud' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickhouseSQL' FROM numbers(1024);

SELECT count() FROM tab WHERE tag LIKE '%Cloud%';
SELECT count() FROM tab WHERE tag LIKE '%Cloud%' ESCAPE '|';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE tag LIKE '%Cloud%' ESCAPE '|'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 3, 4;

SELECT 'A non-ASCII ESCAPE byte is rejected at planning time so the direct-read text-index optimization cannot strip the call';

-- Without the validation in `MergeTreeIndexConditionText`, the text-index analyzer accepts a
-- single non-ASCII byte and the direct-read optimization replaces the `like` call with a
-- virtual-column reference. The execution-layer BAD_ARGUMENTS check never runs and the query
-- silently returns rows. Mirroring the validation in the analyzer makes the error appear at
-- planning time, before any optimization can drop the predicate.
SELECT count() FROM tab WHERE like(tag, '%Cloud%', unhex('FF')); -- { serverError BAD_ARGUMENTS }
EXPLAIN indexes = 1 SELECT count() FROM tab WHERE like(tag, '%Cloud%', unhex('FF')); -- { serverError BAD_ARGUMENTS }

-- An escape argument that is not exactly one byte is rejected on the same path.
SELECT count() FROM tab WHERE like(tag, '%Cloud%', ''); -- { serverError BAD_ARGUMENTS }
EXPLAIN indexes = 1 SELECT count() FROM tab WHERE like(tag, '%Cloud%', ''); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE like(tag, '%Cloud%', 'ab'); -- { serverError BAD_ARGUMENTS }
EXPLAIN indexes = 1 SELECT count() FROM tab WHERE like(tag, '%Cloud%', 'ab'); -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;
