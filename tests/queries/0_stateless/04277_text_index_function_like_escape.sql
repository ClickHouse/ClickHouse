-- Tags: no-parallel-replicas

-- Tests that the text index is still used when the predicate uses
-- `LIKE pattern ESCAPE 'c'` or `ILIKE pattern ESCAPE 'c'`.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 128, index_granularity_bytes = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO tab SELECT number, 'Hello ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello World, ClickHouse is fast!' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo xClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHousez rocks' FROM numbers(1024);
INSERT INTO tab SELECT number, 'literal 50%off token' FROM numbers(1024);
-- The index keeps this row as a candidate for the pattern below only when the ESCAPE clause is ignored.
INSERT INTO tab SELECT number, 'literal 50 discount token' FROM numbers(1024);

SELECT 'Results are the same with and without an ESCAPE clause when the escape character is not used in the pattern';

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

SELECT 'The escaped % is a literal, so the same pattern without its ESCAPE clause matches nothing';

SELECT count() FROM tab WHERE message LIKE '%literal 50#%off token%' ESCAPE '#';
SELECT count() FROM tab WHERE message LIKE '%literal 50#%off token%';
SELECT count() FROM tab WHERE message LIKE '%literal 50\\%off token%';

SELECT 'The index selects 8 of 48 granules for that pattern, the same as for the equivalent backslash-escaped pattern';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%literal 50#%off token%' ESCAPE '#'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%literal 50\\%off token%'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT 'The same pattern without its ESCAPE clause keeps a second part as a candidate and selects 16 of 48 granules';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE message LIKE '%literal 50#%off token%'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT 'Text index analysis with LIKE ESCAPE: index narrows to 1 part / 8 granules out of 6 parts / 48 granules';

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

SELECT 'The ESCAPE form is handed to the index at data-read time too, like the 2-argument form';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tab WHERE like(message, '%World%', '|')
    SETTINGS use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1
) WHERE explain LIKE '%__text_index%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tab WHERE message LIKE '%World%'
    SETTINGS use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1
) WHERE explain LIKE '%__text_index%';

DROP TABLE tab;

SELECT 'Text index with array tokenizer also uses the index with LIKE ESCAPE';

CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 128, index_granularity_bytes = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO tab SELECT number, 'ClickHouseServer' FROM numbers(1024);
INSERT INTO tab SELECT number, 'clickhouseclient' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHouseCloud' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHouseSQL' FROM numbers(1024);

SELECT count() FROM tab WHERE tag LIKE '%Cloud%';
SELECT count() FROM tab WHERE tag LIKE '%Cloud%' ESCAPE '|';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE tag LIKE '%Cloud%' ESCAPE '|'
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 3, 4;

SELECT 'An ESCAPE argument that is not a single ASCII character is rejected, also by EXPLAIN';

SELECT count() FROM tab WHERE like(tag, '%Cloud%', unhex('FF')); -- { serverError BAD_ARGUMENTS }
EXPLAIN indexes = 1 SELECT count() FROM tab WHERE like(tag, '%Cloud%', unhex('FF')); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM tab WHERE like(tag, '%Cloud%', ''); -- { serverError BAD_ARGUMENTS }
EXPLAIN indexes = 1 SELECT count() FROM tab WHERE like(tag, '%Cloud%', ''); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE like(tag, '%Cloud%', 'ab'); -- { serverError BAD_ARGUMENTS }
EXPLAIN indexes = 1 SELECT count() FROM tab WHERE like(tag, '%Cloud%', 'ab'); -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;
