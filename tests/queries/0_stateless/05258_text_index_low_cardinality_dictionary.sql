-- A text index over a LowCardinality column has the same tokens as over a String column with the same values,
-- and searches through it return the same rows as a full scan.

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

-- 300 values with a token repeated within a value, and NULLs.
INSERT INTO tab SELECT number, v, v
FROM (SELECT number, if(number % 50 = 0, NULL, concat('w', toString(number % 300 % 7), ' v', toString(number % 300), ' w', toString(number % 300 % 7))) AS v FROM numbers(3000));

SELECT
    (SELECT count() FROM mergeTreeTextIndex(currentDatabase(), tab, idx_lc)),
    (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_lc))
        = (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s));

SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'w3');
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'w3') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'v299');
SELECT count(), sum(id) FROM tab WHERE hasToken(lc, 'v299') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM tab WHERE hasPhrase(lc, 'v17 w3');
SELECT count(), sum(id) FROM tab WHERE hasPhrase(lc, 'v17 w3') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

-- The same index built by MATERIALIZE INDEX.
ALTER TABLE tab CLEAR INDEX idx_lc, MATERIALIZE INDEX idx_lc SETTINGS mutations_sync = 2;

SELECT
    (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_lc))
        = (SELECT (count(), sum(cityHash64(*))) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s));

-- A postprocessor that drops a token.
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
