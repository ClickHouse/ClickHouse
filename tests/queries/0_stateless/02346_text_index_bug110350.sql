-- Tests for https://github.com/ClickHouse/ClickHouse/issues/110350

-- ILIKE on an expression text index with a lower()/upper() preprocessor must use the index, just like it does for a plain-column index.

SET use_skip_indexes = 1;
SET enable_full_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET explain_query_plan_default = 'legacy';
SET use_query_condition_cache = 0;

CREATE TABLE tab
(
    id Int64,
    commandline1 Nullable(String),
    INDEX idx assumeNotNull(commandline1) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(assumeNotNull(commandline1)))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT number, 'hello world' FROM numbers(2000);
INSERT INTO tab VALUES (2001, 'Some ATISH here'), (2002, 'lower atish token'), (2003, 'MixEd AtIsH case'), (2004, 'no match here'), (2005, NULL);

SELECT 'ILIKE uses the index (force_data_skipping_indices must not throw)';

SELECT count() FROM tab WHERE assumeNotNull(commandline1) ILIKE '%atish%' SETTINGS force_data_skipping_indices = 'idx';

SELECT 'index results match a full scan';

SELECT count() FROM tab WHERE assumeNotNull(commandline1) ILIKE '%atish%';
SELECT count() FROM tab WHERE assumeNotNull(commandline1) ILIKE '%ATISH%';

SELECT count() FROM tab WHERE assumeNotNull(commandline1) ILIKE '%atish%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE assumeNotNull(commandline1) ILIKE '%ATISH%' SETTINGS use_skip_indexes = 0;

SELECT 'the index prunes granules with no matching token';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE assumeNotNull(commandline1) ILIKE '%atish%'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'a non-matching needle prunes every granule';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE assumeNotNull(commandline1) ILIKE '%nonexistent%'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;
