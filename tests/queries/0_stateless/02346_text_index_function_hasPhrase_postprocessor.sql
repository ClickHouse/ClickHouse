-- Tags: no-parallel-replicas

-- Tests hasPhrase over a text index with a postprocessor, with and without support_phrase_search = 1. The postprocessor is applied
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
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

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
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message = 'the', '', message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

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
-- Naming the index tokenizer explicitly gives the same results as the two-argument form above.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see cat', 'splitByNonAlpha');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see the cat', 'splitByNonAlpha');
-- Contrast: a different tokenizer does not match the index, so literal hasPhrase runs and the
-- postprocessor is not applied. Only the literal 'see cat' (3) matches.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'see cat', 'ngrams(3)');

DROP TABLE tab;

SELECT '3. Suffix-stripping postprocessor (changes token sizes): phrase matches on transformed tokens.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = replaceRegexpAll(message, 'ing$', ''), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

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
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message = 'the', '', message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab VALUES
    (1, 'the the the'),
    (2, 'cat the the dog'),
    (3, 'cat dog cat dog'),
    (4, 'the cat the dog the');

SELECT '-- Phrase of only stop words normalizes to an empty phrase and matches nothing';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'the the') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'the the') SETTINGS query_plan_direct_read_from_text_index = 0;
-- An OR keeps the granule alive so the empty phrase is evaluated directly (granule pruning no longer masks
-- it); the empty phrase must still match nothing, so only id = 3 qualifies (not every row via direct read).
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'the the') OR id = 3 SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'the the') OR id = 3 SETTINGS query_plan_direct_read_from_text_index = 0;

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
SETTINGS allow_experimental_text_index_phrase_search = 1;

SYSTEM STOP MERGES tab;

-- Old parts: written before the index, evaluated by the row-scan fallback.
INSERT INTO tab VALUES (1, 'see the cat'), (2, 'see cat');

ALTER TABLE tab ADD INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message = 'the', '', message), support_phrase_search = 1);

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

SELECT '6. ngrams tokenizer + lower postprocessor: case-insensitive substring phrase, consistent across read paths.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = ngrams(3), postprocessor = lower(message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab VALUES
    (1, 'Hello World'),
    (2, 'HELP me'),
    (3, 'say hello');

-- ngrams turn the phrase into a substring search; lower makes it case-insensitive. 'help' shares only
-- the 'hel' gram, so it does not match. The rejoined fallback haystack gains boundary grams from the
-- separators, but the needle grams still appear consecutively only where 'hello' does, so dr on/off agree.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'hello') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'hello') SETTINGS query_plan_direct_read_from_text_index = 0;
-- Needle case does not matter.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'HELLO') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'HELLO') SETTINGS query_plan_direct_read_from_text_index = 0;
-- A substring not present matches nothing.
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'xyz') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'xyz') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT '7. Separator-emitting postprocessor: answered consistently on both read paths.';

SELECT '-- token becomes foo x';
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = concat(message, ' x'), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab VALUES (1, 'foo bar');

SELECT count() FROM tab WHERE hasPhrase(message, 'foo bar') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE hasPhrase(message, 'foo bar') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT '8. Tokenizer whose separators do not include a space.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByString(['()']), postprocessor = lower(message))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

SELECT '-- index tokens are [a, bc, d]';
INSERT INTO tab VALUES (1, 'a()bc()d'), (2, 'zz');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bc()d');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bc()d') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bc()d') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bc()d', 'splitByString([\'()\'])');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bc()d') SETTINGS force_data_skipping_indices = 'idx';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bc');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bc') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a()d');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a()d') SETTINGS use_skip_indexes = 0;
SELECT '-- value outside a filter';
SELECT id, hasPhrase(message, 'bc()d') FROM tab ORDER BY id;

DROP TABLE tab;

SELECT '9. A token that is a separator of splitByNonAlpha but not of the index tokenizer.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByString(['()']), postprocessor = lower(message))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'x-y()z'), (2, 'zz');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'x-y');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'x-y') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'x-y()z');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'x-y()z') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '10. A postprocessed token that is itself a separator.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByString([' ', 'x']), postprocessor = lower(message))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

SELECT '-- index tokens are [a, x, b]';
INSERT INTO tab VALUES (1, 'A X B'), (2, 'zz');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a b');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a b') SETTINGS use_skip_indexes = 0;
SELECT '-- single-token phrase still matches';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '11. asciiCJK tokens may contain non-alphanumeric characters.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = asciiCJK, postprocessor = lower(message))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'A.B C'), (2, 'zz');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a.b');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a.b') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a.b c');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a.b c') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '12. A gram that spans a space.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = ngrams(3), postprocessor = lower(message))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'abcd ef'), (2, 'zzzzzz');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'cd e', 'ngrams(3)');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'cd e', 'ngrams(3)') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bcd', 'ngrams(3)');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bcd', 'ngrams(3)') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '13. With positions, the index and the row-level fallback agree.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByString(['()']), postprocessor = lower(message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab VALUES (1, 'a()bc()d'), (2, 'zz');

SELECT '-- index positions vs row-level fallback';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bc()d') SETTINGS text_index_hint_max_selectivity = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'bc()d') SETTINGS text_index_hint_max_selectivity = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a()d') SETTINGS text_index_hint_max_selectivity = 1;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, 'a()d') SETTINGS text_index_hint_max_selectivity = 0;

DROP TABLE tab;

SELECT '14. An Array(String) indexed column: the tokens of all elements form one sequence.';

CREATE TABLE tab
(
    id UInt32,
    tags Array(String),
    INDEX idx(tags) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(tags))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

SELECT '-- index tokens are [hello, world, foo]';
INSERT INTO tab VALUES (1, ['Hello World', 'Foo']), (2, ['zz']);

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(tags, 'hello world');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(tags, 'hello world') SETTINGS use_skip_indexes = 0;
SELECT '-- adjacent across elements';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(tags, 'world foo');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(tags, 'world foo') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(tags, 'hello foo');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(tags, 'hello foo') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '15. A NULL array element leaves no position.';

CREATE TABLE tab
(
    id UInt32,
    tags Array(Nullable(String)),
    INDEX idx(tags) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(tags))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, ['Hello', NULL, 'World']), (2, ['zz']);

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(tags, 'hello world');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(tags, 'hello world') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '16. An expression index.';

CREATE TABLE tab
(
    id UInt32,
    message Nullable(String),
    INDEX idx(ifNull(message, 'default')) TYPE text(tokenizer = splitByString(['()']), postprocessor = lower(ifNull(message, 'default')))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'A()BC()D'), (2, NULL);

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(ifNull(message, 'default'), 'bc()d');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(ifNull(message, 'default'), 'bc()d') SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(ifNull(message, 'default'), 'a()d');
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(ifNull(message, 'default'), 'a()d') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
SELECT '17. An Array phrase is a sequence.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(message))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'A B A'), (2, 'A A B'), (3, 'zz');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, ['a', 'b', 'a']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, ['a', 'b', 'a']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, ['a', 'a', 'b']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, ['a', 'a', 'b']) SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '18. An Array phrase with positions.';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab VALUES (1, 'The QUICK Brown fox'), (2, 'The Brown QUICK fox'), (3, 'zz');

SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, ['quick', 'brown']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, ['quick', 'brown']) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, ['quick', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasPhrase(message, ['quick', 'fox']) SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

DROP TABLE IF EXISTS tab;
