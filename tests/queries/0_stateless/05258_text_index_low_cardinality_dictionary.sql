-- A text index over a LowCardinality column has the same tokens as over a String column with the same values,
-- and searches through it return the same rows as a full scan.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;

CREATE TABLE tab
(
    id UInt64,
    lc LowCardinality(Nullable(String)),
    s Nullable(String),
    INDEX idx_lc lc TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1),
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 104857600, allow_experimental_text_index_phrase_search = 1;

SELECT '300 values with a token repeated within a value, and NULLs';

INSERT INTO tab SELECT number, v, v
FROM (
    SELECT number, if(number % 50 = 0, NULL, concat('w', toString(number % 300 % 7), ' v', toString(number % 300), ' w', toString(number % 300 % 7))) AS v
    FROM numbers(3000)
);

SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_lc);
SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s);

SELECT 'Search vs full scan';
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'w3');
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'w3') SETTINGS use_skip_indexes = 0;
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'v299');
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'v299') SETTINGS use_skip_indexes = 0;
SELECT count(), sum(id) FROM tab WHERE hasPhrase(lc, 'v17 w3');
SELECT count(), sum(id) FROM tab WHERE hasPhrase(lc, 'v17 w3') SETTINGS use_skip_indexes = 0;

SELECT 'MATERIALIZE INDEX';

ALTER TABLE tab CLEAR INDEX idx_lc, MATERIALIZE INDEX idx_lc SETTINGS mutations_sync = 2;

SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_lc);
SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s);

SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'w3');
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'w3') SETTINGS use_skip_indexes = 0;
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'v299');
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'v299') SETTINGS use_skip_indexes = 0;
SELECT count(), sum(id) FROM tab WHERE hasPhrase(lc, 'v17 w3');
SELECT count(), sum(id) FROM tab WHERE hasPhrase(lc, 'v17 w3') SETTINGS use_skip_indexes = 0;

SELECT 'Arrays with NULL and empty elements, a value repeated within a row, and empty arrays';

CREATE TABLE tab_array
(
    id UInt64,
    lc Array(LowCardinality(Nullable(String))),
    s Array(Nullable(String)),
    INDEX idx_lc lc TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1),
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 104857600, allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_array SELECT number, v, v
FROM (SELECT number, if(number % 10 = 0, [], arrayMap(k -> multiIf(k = 0, NULL, k = 1, '', concat('e', toString((number + k) % 13), ' f')), [0, 1, 2, 3, 3])) AS v FROM numbers(1000));

SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab_array, idx_lc);
SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab_array, idx_s);

SELECT count(), sum(id) FROM tab_array WHERE hasAnyTokens(lc, ['e3']);
SELECT count(), sum(id) FROM tab_array WHERE hasAnyTokens(lc, ['e3']) SETTINGS use_skip_indexes = 0;

SELECT 'Stateful tokenizer';
CREATE TABLE tab_sparse_grams
(
    lc LowCardinality(String),
    s String,
    INDEX idx_lc lc TYPE text(tokenizer = sparseGrams(3, 8)),
    INDEX idx_s s TYPE text(tokenizer = sparseGrams(3, 8))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab_sparse_grams SELECT v, v FROM (SELECT concat('abcd', toString(number % 30)) AS v FROM numbers(1000));

SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab_sparse_grams, idx_lc);
SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab_sparse_grams, idx_s);

SELECT 'Threshold: 80 rows tokenize each value once, 79 rows tokenize every row';

CREATE TABLE tab_threshold
(
    lc LowCardinality(String),
    s String,
    INDEX idx_lc lc TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY tuple();

SYSTEM STOP MERGES tab_threshold;
INSERT INTO tab_threshold SELECT v, v FROM (SELECT concat('c', toString(number % 9)) AS v FROM numbers(80));
INSERT INTO tab_threshold SELECT v, v FROM (SELECT concat('c', toString(number % 9)) AS v FROM numbers(79));

SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab_threshold, idx_lc);
SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab_threshold, idx_s);

SELECT 'Merge';

CREATE TABLE tab_merge_lc
(
    id UInt64,
    v LowCardinality(String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, index_granularity_bytes = 104857600, text_index_max_processed_tokens_before_flush = 1000;

CREATE TABLE tab_merge_s
(
    id UInt64,
    v String
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, index_granularity_bytes = 104857600, text_index_max_processed_tokens_before_flush = 1000;

INSERT INTO tab_merge_lc SELECT number, concat('a', toString(number % 50 % 5), ' b', toString(number % 50)) FROM numbers(1500);
INSERT INTO tab_merge_lc SELECT number, concat('a', toString(number % 50 % 5), ' b', toString(number % 50)) FROM numbers(1500, 1500);
INSERT INTO tab_merge_s SELECT * FROM tab_merge_lc WHERE id < 1500;
INSERT INTO tab_merge_s SELECT * FROM tab_merge_lc WHERE id >= 1500;

ALTER TABLE tab_merge_lc ADD INDEX idx v TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE tab_merge_s ADD INDEX idx v TYPE text(tokenizer = splitByNonAlpha);
OPTIMIZE TABLE tab_merge_lc FINAL;
OPTIMIZE TABLE tab_merge_s FINAL;

SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab_merge_lc, idx);
SELECT count(), sum(cityHash64(token, cardinality)) FROM mergeTreeTextIndex(currentDatabase(), tab_merge_s, idx);

SELECT 'Postprocessor that drops a token';

CREATE TABLE tab_postprocessor
(
    id UInt64,
    lc LowCardinality(String),
    INDEX idx lc TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(lc IN ('w1'), '', lc))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_postprocessor SELECT number, concat('w', toString(number % 3)) FROM numbers(1000);

SELECT arraySort(groupArray(token)) FROM mergeTreeTextIndex(currentDatabase(), tab_postprocessor, idx);
SELECT count(), sum(id) FROM tab_postprocessor WHERE hasToken(lc, 'w2');

DROP TABLE tab;
DROP TABLE tab_postprocessor;
DROP TABLE tab_array;
DROP TABLE tab_sparse_grams;
DROP TABLE tab_threshold;
DROP TABLE tab_merge_lc;
DROP TABLE tab_merge_s;
