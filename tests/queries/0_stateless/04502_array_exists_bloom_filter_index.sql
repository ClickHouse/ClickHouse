-- Tags: no-parallel-replicas
-- Tests that `tokenbf_v1` and `ngrambf_v1` indexes can be used through simple `arrayExists` lambdas.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
-- Otherwise `arrayExists(x -> x = c, arr)` is rewritten to `has(arr, c)` before index
-- analysis and the equals sections below would test `has` instead of `arrayExists`.
SET optimize_rewrite_array_exists_to_has = 0;

DROP TABLE IF EXISTS tab_token;

CREATE TABLE tab_token
(
    id UInt32,
    arr Array(String),
    needle String,
    INDEX idx(arr) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_token VALUES
    (1, ['abc def foo'], '%foo%'),
    (2, ['abc def bar'], '%bar%'),
    (3, ['foo', 'baz'], '%foo%'),
    (4, ['xyz'], '%xyz%'),
    (5, ['foo', 'bar'], '%foo bar%'),
    (6, [], '');

SELECT 'arrayExists with LIKE uses the token bloom filter index';
SELECT id FROM tab_token WHERE arrayExists(x -> x LIKE '%foo%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab_token WHERE arrayExists(x -> x LIKE '%missing%', arr) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'the index prunes granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_token WHERE arrayExists(x -> x = 'xyz', arr)
) WHERE explain LIKE '%Granules:%' LIMIT 1, 1;

SELECT 'captured constants are recognized';
WITH '%foo%' AS pattern SELECT id FROM tab_token WHERE arrayExists(x -> x LIKE pattern, arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'the original arrayExists predicate is still applied after the index check';
SELECT id FROM tab_token WHERE arrayExists(x -> x LIKE '%foo bar%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'arrayExists with equals uses the index (both operand orders)';
SELECT id FROM tab_token WHERE arrayExists(x -> x = 'xyz', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab_token WHERE arrayExists(x -> 'xyz' = x, arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'arrayExists with hasToken, startsWith, endsWith, multiSearchAny and match uses the index';
SELECT id FROM tab_token WHERE arrayExists(x -> hasToken(x, 'foo'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab_token WHERE arrayExists(x -> startsWith(x, 'xyz'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab_token WHERE arrayExists(x -> endsWith(x, 'az'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab_token WHERE arrayExists(x -> multiSearchAny(x, ['xyz', 'baz']), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab_token WHERE arrayExists(x -> match(x, 'xyz|baz'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'arrayExists with IN over a constant set uses the index';
SELECT id FROM tab_token WHERE arrayExists(x -> x IN ('xyz', 'baz'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_token WHERE arrayExists(x -> x IN ('xyz', 'baz'), arr)
) WHERE explain LIKE '%Granules:%' LIMIT 1, 1;

SELECT 'negated arrayExists stays correct (empty arrays match)';
SELECT id FROM tab_token WHERE NOT arrayExists(x -> x = 'foo', arr) ORDER BY id;

SELECT 'negative functions inside the lambda do not use the index (empty arrays would be pruned wrongly)';
SELECT id FROM tab_token WHERE arrayExists(x -> x != 'foo', arr) ORDER BY id;
SELECT id FROM tab_token WHERE arrayExists(x -> x != 'foo', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab_token WHERE arrayExists(x -> x NOT LIKE '%foo%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab_token WHERE arrayExists(x -> x NOT IN ('foo', 'xyz'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'functions not supported by the bloom filter index do not use it';
SELECT id FROM tab_token WHERE arrayExists(x -> x ILIKE '%FOO%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab_token WHERE arrayExists(x -> multiMatchAny(x, ['xyz', 'baz']), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab_token WHERE arrayExists(x -> hasAnyTokens(x, ['foo']), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab_token WHERE arrayExists(x -> hasAllTokens(x, ['foo']), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab_token WHERE arrayExists(x -> hasPhrase(x, 'abc def'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'non-constant lambda predicates do not use the index';
SELECT id FROM tab_token WHERE arrayExists(x -> x LIKE needle, arr) ORDER BY id;
SELECT id FROM tab_token WHERE arrayExists(x -> x LIKE needle, arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'unsupported lambda shapes do not use the index';
SELECT id FROM tab_token WHERE arrayExists(x -> (x LIKE '%foo%') OR (x = 'xyz'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab_token WHERE arrayExists(x -> lower(x) LIKE '%foo%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab_token WHERE arrayExists((x, y) -> x LIKE '%foo%', arr, arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab_token;

SELECT 'ngram bloom filter index works through arrayExists too';

DROP TABLE IF EXISTS tab_ngram;

CREATE TABLE tab_ngram
(
    id UInt32,
    arr Array(String),
    INDEX idx(arr) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_ngram VALUES (1, ['abc def foo']), (2, ['xyz']), (3, []);

SELECT id FROM tab_ngram WHERE arrayExists(x -> x LIKE '%foo%', arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab_ngram WHERE arrayExists(x -> hasToken(x, 'foo'), arr) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'LIKE prunes granules on the ngram index';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_ngram WHERE arrayExists(x -> x LIKE '%foo%', arr)
) WHERE explain LIKE '%Granules:%' LIMIT 1, 1;

DROP TABLE tab_ngram;
