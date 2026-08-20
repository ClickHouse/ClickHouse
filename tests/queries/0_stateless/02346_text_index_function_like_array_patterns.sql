-- Tags: no-parallel-replicas

-- Tests arbitrary LIKE/ILIKE patterns on a text index with the `array` tokenizer, where a token is the value.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_rewrite_like_perfect_affix = 0; -- avoid rewrite of some patterns into startsWith/endsWith

SELECT 'Accepts arbitrary patterns for LIKE queries';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    name String,
    INDEX idx(name) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, name) VALUES
    (1, 'alpha-service-prod'),
    (2, 'beta-service-prod'),
    (3, 'gamma-svc-4999-dev'),
    (4, 'alpha_service_prod'),
    (5, 'prod-alpha-service'),
    (6, '100%-alpha-prod'),
    (7, 'under_score_prod'),
    (8, '');

SELECT '-- without optimization';
SET use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT groupArray(id) FROM tab WHERE name LIKE '%4999%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%svc-4999%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%prod';
SELECT groupArray(id) FROM tab WHERE name LIKE '%alpha%prod%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha_service%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha-service-prod';
SELECT groupArray(id) FROM tab WHERE name LIKE '%100\%%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%\_score\_%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'nonexistent%';
SELECT groupArray(id) FROM tab WHERE name NOT LIKE '%prod';
SELECT groupArray(id) FROM tab WHERE name NOT LIKE 'alpha%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%' AND name LIKE '%prod';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%' OR name LIKE '%4999%';
SELECT groupArray(id) FROM tab WHERE name ILIKE '%ALPHA%PROD%';
SELECT groupArray(id) FROM tab WHERE name ILIKE 'ALPHA%';
SELECT groupArray(id) FROM tab WHERE name ILIKE '%PROD';

SELECT '-- with optimization';
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT groupArray(id) FROM tab WHERE name LIKE '%4999%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%svc-4999%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%prod';
SELECT groupArray(id) FROM tab WHERE name LIKE '%alpha%prod%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha_service%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha-service-prod';
SELECT groupArray(id) FROM tab WHERE name LIKE '%100\%%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%\_score\_%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%nonexistent%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'nonexistent%';
SELECT groupArray(id) FROM tab WHERE name NOT LIKE '%prod';
SELECT groupArray(id) FROM tab WHERE name NOT LIKE 'alpha%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%' AND name LIKE '%prod';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%' OR name LIKE '%4999%';
SELECT groupArray(id) FROM tab WHERE name ILIKE '%ALPHA%PROD%';
SELECT groupArray(id) FROM tab WHERE name ILIKE 'ALPHA%';
SELECT groupArray(id) FROM tab WHERE name ILIKE '%PROD';

SELECT 'The pattern is decided by the index alone, so the original condition is removed';

-- Columns: does the plan have a text index virtual column, does it still evaluate the original function.
SELECT 'infix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION like(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE '%svc-4999%');

SELECT 'prefix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION like(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE 'alpha%');

SELECT 'suffix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION like(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE '%prod');

SELECT 'multiple needles', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION like(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE '%alpha%prod%');

SELECT 'underscore', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION like(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE 'alpha_service%');

SELECT 'wildcard-free', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION like(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE 'alpha-service-prod');

SELECT 'ilike', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION ilike(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name ILIKE '%ALPHA%PROD%');

SELECT 'A pattern with too little literal content is not evaluated by a dictionary scan';

-- Non-wildcard characters are counted across the whole pattern, so several needles must not score below one.
SELECT 'too little', countIf(explain LIKE '%\_\_text_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE 'a%b%c');

SELECT 'enough', countIf(explain LIKE '%\_\_text_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE 'a%prod');

SELECT 'single short needle', countIf(explain LIKE '%\_\_text_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE '%svc%');

SELECT 'several short needles', countIf(explain LIKE '%\_\_text_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE '%svc%dev%');

SELECT groupArray(id) FROM tab WHERE name LIKE 'a%b%c';
SELECT groupArray(id) FROM tab WHERE name LIKE 'a%prod';
SELECT groupArray(id) FROM tab WHERE name LIKE '%svc%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%svc%dev%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%svc%dev%' SETTINGS use_skip_indexes = 0;

SELECT 'A pattern without a literal character is left to the original condition';

-- Required even at text_index_like_min_pattern_length = 0: the empty string has no token.
SELECT 'all wildcards', countIf(explain LIKE '%\_\_text_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE '%%' SETTINGS text_index_like_min_pattern_length = 0);

SELECT 'single underscore', countIf(explain LIKE '%\_\_text_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE '_%' SETTINGS text_index_like_min_pattern_length = 0);

SELECT groupArray(id) FROM tab WHERE name LIKE '%' SETTINGS text_index_like_min_pattern_length = 0;
SELECT groupArray(id) FROM tab WHERE name LIKE '%' SETTINGS text_index_like_min_pattern_length = 0, use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE name LIKE '' SETTINGS text_index_like_min_pattern_length = 0;
SELECT groupArray(id) FROM tab WHERE name LIKE '' SETTINGS text_index_like_min_pattern_length = 0, use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE name LIKE '_%' SETTINGS text_index_like_min_pattern_length = 0;
SELECT groupArray(id) FROM tab WHERE name LIKE '_%' SETTINGS text_index_like_min_pattern_length = 0, use_skip_indexes = 0;

SELECT 'An invalid pattern raises';

SELECT count() FROM tab WHERE name LIKE 'alpha\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

DROP TABLE tab;

SELECT 'Granule pruning';

CREATE TABLE tab
(
    id UInt32,
    name String,
    INDEX idx(name) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT number, 'alpha-service-prod' FROM numbers(1024);
INSERT INTO tab SELECT number, 'beta-service-prod' FROM numbers(1024);
INSERT INTO tab SELECT number, 'gamma-svc-4999-dev' FROM numbers(1024);
INSERT INTO tab SELECT number, 'delta-service-dev' FROM numbers(1024);

SELECT '-- A prefix pattern should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE name LIKE 'alpha%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- A suffix pattern should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE name LIKE '%service-prod'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- A pattern with several needles should choose 1 part and 1024 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE name LIKE '%service%dev%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- A non-existent needle should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE name LIKE 'nonexistent%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT count() FROM tab WHERE name LIKE 'alpha%';
SELECT count() FROM tab WHERE name LIKE 'alpha%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE name LIKE '%service-prod';
SELECT count() FROM tab WHERE name LIKE '%service-prod' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE name LIKE '%service%dev%';
SELECT count() FROM tab WHERE name LIKE '%service%dev%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'A NULL value is not returned by a negated pattern';

CREATE TABLE tab
(
    id UInt32,
    name Nullable(String),
    INDEX idx(name) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, name) VALUES
    (1, 'alpha-service-prod'),
    (2, 'beta-service-prod'),
    (3, NULL),
    (4, '');

SELECT groupArray(id) FROM tab WHERE name LIKE '%service%prod%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%service%prod%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE name NOT LIKE '%service%prod%';
SELECT groupArray(id) FROM tab WHERE name NOT LIKE '%service%prod%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE name NOT LIKE 'alpha%';
SELECT groupArray(id) FROM tab WHERE name NOT LIKE 'alpha%' SETTINGS use_skip_indexes = 0;

-- A UInt8 virtual column cannot carry the NULL a predicate returns for a NULL value, which `NOT` would flip to
-- true. `like` is safe: inversion push-down folds `not(like(...))` into the unsupported `notLike`, so the node
-- is never replaced.
SELECT 'not like', countIf(explain LIKE '%\_\_text_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name NOT LIKE '%service%prod%');

DROP TABLE tab;

SELECT 'A FixedString value keeps its zero padding, which the pattern matches against';

CREATE TABLE tab
(
    id UInt32,
    name FixedString(12),
    INDEX idx(name) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, name) VALUES
    (1, 'alpha-prod'),
    (2, 'beta-prod'),
    (3, 'gamma-4999-x');

SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE name LIKE '%4999-x';
SELECT groupArray(id) FROM tab WHERE name LIKE '%4999-x' SETTINGS use_skip_indexes = 0;
-- The stored value is 'alpha-prod' plus two zero bytes, so it does not end with 'prod'.
SELECT groupArray(id) FROM tab WHERE name LIKE '%-prod';
SELECT groupArray(id) FROM tab WHERE name LIKE '%-prod' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'A LowCardinality value is supported';

CREATE TABLE tab
(
    id UInt32,
    name LowCardinality(String),
    INDEX idx(name) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, name) VALUES
    (1, 'alpha-service-prod'),
    (2, 'beta-service-prod'),
    (3, 'gamma-svc-4999-dev');

SELECT groupArray(id) FROM tab WHERE name LIKE '%service%prod%';
SELECT groupArray(id) FROM tab WHERE name LIKE '%service%prod%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'alpha%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'A case folding preprocessor is supported for ILIKE';

CREATE TABLE tab
(
    id UInt32,
    name String,
    INDEX idx(name) TYPE text(tokenizer = array, preprocessor = lower(name))
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, name) VALUES
    (1, 'Alpha-Service-Prod'),
    (2, 'BETA-SERVICE-PROD'),
    (3, 'gamma-svc-4999-dev');

SELECT groupArray(id) FROM tab WHERE name ILIKE '%SERVICE%PROD%';
SELECT groupArray(id) FROM tab WHERE name ILIKE '%SERVICE%PROD%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE name ILIKE 'alpha%';
SELECT groupArray(id) FROM tab WHERE name ILIKE 'alpha%' SETTINGS use_skip_indexes = 0;
-- A preprocessor rewrites the stored token, so a case-sensitive pattern is left to the original condition.
SELECT groupArray(id) FROM tab WHERE name LIKE 'Alpha%';
SELECT groupArray(id) FROM tab WHERE name LIKE 'Alpha%' SETTINGS use_skip_indexes = 0;

SELECT 'like', countIf(explain LIKE '%\_\_text_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name LIKE 'Alpha%');

SELECT 'ilike', countIf(explain LIKE '%\_\_text_index\_%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE name ILIKE 'alpha%');

DROP TABLE tab;

SELECT 'The pattern falls back to the original condition when the scan reads too many posting lists';

CREATE TABLE tab
(
    id UInt64,
    name String,
    INDEX idx(name) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

-- Each value is shared by many rows, so its posting list is stored out of line and counts towards the limit.
INSERT INTO tab SELECT number, format('service-{}-prod', number % 1000) FROM numbers(100000);

SELECT count() FROM tab WHERE name LIKE '%service%prod%' SETTINGS text_index_like_max_postings_to_read = 1;
SELECT count() FROM tab WHERE name LIKE '%service%prod%' SETTINGS text_index_like_max_postings_to_read = 1, use_skip_indexes = 0;
SELECT count() FROM tab WHERE name LIKE 'service-42-%' SETTINGS text_index_like_max_postings_to_read = 1;
SELECT count() FROM tab WHERE name LIKE 'service-42-%' SETTINGS text_index_like_max_postings_to_read = 1, use_skip_indexes = 0;

DROP TABLE tab;
