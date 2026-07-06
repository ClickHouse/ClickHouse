-- Tags: no-parallel-replicas, no-azure-blob-storage
-- Tests that text indexes can be used through simple `arrayExists` lambdas.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
-- Otherwise `arrayExists(x -> x = c, arr)` is rewritten to `has(arr, c)` before index
-- analysis and the equals sections below would not exercise the `arrayExists` path.
SET optimize_rewrite_array_exists_to_has = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    arr Array(String),
    needle String,
    INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, text_index_posting_list_block_size = 10000;

INSERT INTO tab VALUES
    (1, ['abc def foo'], '%foo%'),
    (2, ['abc def bar'], '%bar%'),
    (3, ['foo', 'baz'], '%foo%'),
    (4, ['xyz'], '%xyz%'),
    (5, ['foo', 'bar'], '%foo bar%'),
    (6, [], '');

SELECT 'arrayExists with LIKE uses the text index';
SELECT id FROM tab WHERE arrayExists(x -> x LIKE '%foo%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab WHERE arrayExists(x -> x LIKE '%bar%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab WHERE arrayExists(x -> x LIKE '%missing%', arr) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'the index prunes granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE arrayExists(x -> x = 'xyz', arr)
) WHERE explain LIKE '%Granules:%' LIMIT 1, 1;

SELECT 'captured constants are recognized';
WITH '%foo%' AS pattern SELECT id FROM tab WHERE arrayExists(x -> x LIKE pattern, arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'the original arrayExists predicate is still applied after the index hint';
SELECT id FROM tab WHERE arrayExists(x -> x LIKE '%foo bar%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'arrayExists with equals uses the text index';
SELECT id FROM tab WHERE arrayExists(x -> x = 'xyz', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab WHERE arrayExists(x -> 'xyz' = x, arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'arrayExists with hasToken uses the text index when the index matches hasToken semantics';
SELECT id FROM tab WHERE arrayExists(x -> hasToken(x, 'foo'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'negated arrayExists stays correct';
SELECT id FROM tab WHERE NOT arrayExists(x -> x LIKE '%foo%', arr) ORDER BY id;

SELECT 'non-constant lambda predicates do not use the index';
SELECT id FROM tab WHERE arrayExists(x -> x LIKE needle, arr) ORDER BY id;
SELECT id FROM tab WHERE arrayExists(x -> x LIKE needle, arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'unsupported lambda shapes do not use the index';
SELECT id FROM tab WHERE arrayExists(x -> (x LIKE '%foo%') OR (x = 'xyz'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab WHERE arrayExists(x -> lower(x) LIKE '%foo%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab WHERE arrayExists(x -> length(x) > 2, arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab WHERE arrayExists((x, y) -> x LIKE '%foo%', arr, arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab;

SELECT 'token-semantics functions are rejected when the index tokenizer differs from theirs';

DROP TABLE IF EXISTS tab_ngrams;

CREATE TABLE tab_ngrams
(
    id UInt32,
    arr Array(String),
    INDEX idx(arr) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_ngrams VALUES (1, ['abc def foo']), (2, ['xyz']);

SELECT id FROM tab_ngrams WHERE arrayExists(x -> x LIKE '%foo%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab_ngrams WHERE arrayExists(x -> match(x, 'foo|xyz'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab_ngrams WHERE arrayExists(x -> hasToken(x, 'foo'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'LIKE and match prune granules on the ngrams index';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_ngrams WHERE arrayExists(x -> x LIKE '%foo%', arr)
) WHERE explain LIKE '%Granules:%' LIMIT 1, 1;
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_ngrams WHERE arrayExists(x -> match(x, 'xyz'), arr)
) WHERE explain LIKE '%Granules:%' LIMIT 1, 1;

DROP TABLE tab_ngrams;

SELECT 'token-semantics functions are rejected when the index has a preprocessor';

DROP TABLE IF EXISTS tab_preprocessed;

CREATE TABLE tab_preprocessed
(
    id UInt32,
    arr Array(String),
    INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(arr)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_preprocessed VALUES (1, ['ABC DEF FOO']), (2, ['xyz']);

SELECT id FROM tab_preprocessed WHERE arrayExists(x -> x LIKE '%FOO%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab_preprocessed WHERE arrayExists(x -> hasToken(x, 'FOO'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab_preprocessed;
