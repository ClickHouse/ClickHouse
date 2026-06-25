-- Tags: no-parallel-replicas

-- Tests hasPhrase over a text index with positions = 1 AND a postprocessor. The postprocessor is applied
-- to both the indexed tokens and the phrase needle, and tokens dropped by the postprocessor leave no
-- positional gap (the index assigns dense positions), so phrase matching reflects the postprocessed token
-- sequence. Every match must be identical whether the index is read directly
-- (query_plan_direct_read_from_text_index = 1) or via the row-scan fallback (= 0), including on partially
-- materialized indexes.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

SELECT '1. lower postprocessor: phrase search is case-insensitive.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(message), positions = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

INSERT INTO tab VALUES
    (1, 'Quick Brown Fox'),
    (2, 'brown quick fox'),
    (3, 'THE QUICK BROWN');

-- 'quick brown' matches rows whose lowercased tokens contain the consecutive phrase.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'quick brown') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'quick brown') SETTINGS query_plan_direct_read_from_text_index = 0;
-- Needle case does not matter (the needle is lowercased too).
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'QUICK BROWN') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'QUICK BROWN') SETTINGS query_plan_direct_read_from_text_index = 0;
-- Wrong order does not match.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'brown quick') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'brown quick') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT '2. Stop-word postprocessor: dropped tokens do not break adjacency.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message = 'the', '', message), positions = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

INSERT INTO tab VALUES
    (1, 'see the cat'),
    (2, 'see a cat'),
    (3, 'see cat'),
    (4, 'the cat see'),
    (5, 'cat see');

-- 'the' is dropped, so 'see cat' and 'see the cat' are the same phrase and match both 'see the cat' (1)
-- and 'see cat' (3). 'see a cat' (2) keeps the non-stop-word 'a', so it is not adjacent.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see the cat') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see the cat') SETTINGS query_plan_direct_read_from_text_index = 0;
-- Contrast: the 3-argument form bypasses the index and runs literal hasPhrase (no postprocessor), so
-- 'see cat' only matches the literal 'see cat' (3), and 'see the cat' only the literal 'see the cat' (1).
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see cat', 'splitByNonAlpha');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see the cat', 'splitByNonAlpha');

DROP TABLE tab;

SELECT '3. Suffix-stripping postprocessor (changes token sizes): phrase matches on transformed tokens.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = replaceRegexpAll(message, 'ing$', ''), positions = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

INSERT INTO tab VALUES
    (1, 'running walking fast'),
    (2, 'walking running fast'),
    (3, 'runn walk');

-- Each token has the 'ing' suffix stripped: 'running walking' -> 'runn walk'.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'running walking') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'running walking') SETTINGS query_plan_direct_read_from_text_index = 0;
-- The needle is stripped too, so the already-stripped phrase 'runn walk' matches the same rows.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'runn walk') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'runn walk') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT '4. Corner cases.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message = 'the', '', message), positions = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

INSERT INTO tab VALUES
    (1, 'the the the'),
    (2, 'cat the the dog'),
    (3, 'cat dog cat dog'),
    (4, 'the cat the dog the');

SELECT '-- Phrase of only stop words: the index bails out, the literal row scan matches literal occurrences';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'the the') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'the the') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- Stop words between real tokens are removed, so cat dog matches across a dropped the';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'cat dog') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'cat dog') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- Duplicate real tokens in the phrase keep their multiplicity';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'cat dog cat dog') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'cat dog cat dog') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT '5. Partially materialized index: row-scan (old parts) and index (new parts) agree.';

CREATE TABLE tab (id UInt32, message String)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

SYSTEM STOP MERGES tab;

-- Old parts: written before the index, evaluated by the row-scan fallback.
INSERT INTO tab VALUES (1, 'see the cat'), (2, 'see cat');

ALTER TABLE tab ADD INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message = 'the', '', message), positions = 1);

-- New parts: written after the index, eligible for index lookup.
INSERT INTO tab VALUES (3, 'see the cat'), (4, 'see cat');

-- 'see cat' must match the old 'see the cat' (1) and 'see cat' (2) and the new ones (3, 4) alike,
-- regardless of which parts have the index materialized or whether direct read is used.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see the cat');

ALTER TABLE tab MATERIALIZE INDEX idx;

-- After full materialization the result is unchanged.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see cat');

SYSTEM START MERGES tab;
DROP TABLE tab;

DROP TABLE IF EXISTS tab;
