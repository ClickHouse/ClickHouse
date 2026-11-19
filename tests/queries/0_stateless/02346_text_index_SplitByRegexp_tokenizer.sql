-- Tags: no-parallel-replicas

-- Tests the `splitByRegexp` tokenizer for text indexes: build a text index whose tokenizer splits on a
-- regular expression separator, then verify that index-backed search (`hasAnyTokens` / `hasAllTokens`)
-- returns exactly the expected rows - no more, no less. `force_data_skipping_indices` makes the queries
-- fail unless the text index is actually used, so every search below is served by the index.

-- 1. Simple separator: split on a single literal comma.

DROP TABLE IF EXISTS tab_regex_simple;

CREATE TABLE tab_regex_simple
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByRegexp(','))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_regex_simple VALUES (1, 'apple,banana,cherry'), (2, 'banana,date'), (3, 'cherry,elderberry'), (4, 'fig,grape');

SELECT 'simple: hasAnyTokens([banana]) -> 1, 2';
SELECT id FROM tab_regex_simple WHERE hasAnyTokens(doc, ['banana']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'simple: hasAnyTokens([cherry, date]) -> 1, 2, 3';
SELECT id FROM tab_regex_simple WHERE hasAnyTokens(doc, ['cherry', 'date']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'simple: hasAllTokens([banana, cherry]) -> 1';
SELECT id FROM tab_regex_simple WHERE hasAllTokens(doc, ['banana', 'cherry']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'simple: hasAnyTokens([watermelon]) -> (none)';
SELECT id FROM tab_regex_simple WHERE hasAnyTokens(doc, ['watermelon']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_regex_simple;

-- 2. Character class `[ ,;]`: split on a space, comma or semicolon.

DROP TABLE IF EXISTS tab_regex_class;

CREATE TABLE tab_regex_class
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByRegexp('[ ,;]'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_regex_class VALUES (1, 'red green;blue'), (2, 'green,yellow orange'), (3, 'blue;purple,pink');

SELECT 'class: hasAnyTokens([green]) -> 1, 2';
SELECT id FROM tab_regex_class WHERE hasAnyTokens(doc, ['green']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'class: hasAnyTokens([blue]) -> 1, 3';
SELECT id FROM tab_regex_class WHERE hasAnyTokens(doc, ['blue']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'class: hasAllTokens([blue, pink]) -> 3';
SELECT id FROM tab_regex_class WHERE hasAllTokens(doc, ['blue', 'pink']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'class: hasAnyTokens([cyan]) -> (none)';
SELECT id FROM tab_regex_class WHERE hasAnyTokens(doc, ['cyan']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_regex_class;

-- 3. Negated character class `[^a-z]+`: split on any run of characters that are not lower-case letters
-- (digits, spaces, punctuation and upper-case letters are all separators, so e.g. `WORLD` is consumed
-- entirely as a separator and never becomes a token).

DROP TABLE IF EXISTS tab_regex_negation;

CREATE TABLE tab_regex_negation
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByRegexp('[^a-z]+'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_regex_negation VALUES (1, 'foo123bar456baz'), (2, 'hello WORLD test'), (3, 'qux-quux_corge');

SELECT 'negation: tokens of each row';
SELECT id, arraySort(tokens(doc, $$splitByRegexp('[^a-z]+')$$)) FROM tab_regex_negation ORDER BY id;

SELECT 'negation: hasAnyTokens([bar]) -> 1';
SELECT id FROM tab_regex_negation WHERE hasAnyTokens(doc, ['bar']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'negation: hasAnyTokens([test]) -> 2';
SELECT id FROM tab_regex_negation WHERE hasAnyTokens(doc, ['test']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'negation: hasAnyTokens([world]) -> (none, WORLD was a separator)';
SELECT id FROM tab_regex_negation WHERE hasAnyTokens(doc, ['world']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'negation: hasAllTokens([qux, corge]) -> 3';
SELECT id FROM tab_regex_negation WHERE hasAllTokens(doc, ['qux', 'corge']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_regex_negation;

-- 4. Issue #103783: preserve special-character tokens such as `C++` and `C#`, which `splitByNonAlpha`
-- would reduce to `c` (causing false positives). The separator is any run of characters that are not
-- letters, digits, `#` or `+`, so `C++` and `C#` are kept as single tokens and can be searched exactly.

DROP TABLE IF EXISTS tab_special_tokens;

CREATE TABLE tab_special_tokens
(
    id UInt64,
    description String,
    INDEX idx description TYPE text(tokenizer = splitByRegexp('[^\\p{L}\\p{N}#+]+'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_special_tokens VALUES
    (1, 'We use C++ for our backend systems'),
    (2, 'Built with C# and React'),
    (3, 'C is our primary language'),
    (4, 'Learning the C language basics');

SELECT 'special: hasAnyTokens([C++]) -> 1 (no false positives)';
SELECT id FROM tab_special_tokens WHERE hasAnyTokens(description, ['C++']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'special: hasAnyTokens([C#]) -> 2';
SELECT id FROM tab_special_tokens WHERE hasAnyTokens(description, ['C#']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'special: hasAnyTokens([C]) -> 3, 4 (plain C only)';
SELECT id FROM tab_special_tokens WHERE hasAnyTokens(description, ['C']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_special_tokens;

-- 5. Issue #103783 (`hasPhrase`): with `splitByRegexp` the phrase `C# and` must match only a row that
-- actually contains the token `C#`, not one containing `C++`. With `splitByNonAlpha` both `C#` and `C++`
-- collapse to `c`, so `hasPhrase(description, 'C# and')` would falsely match `... C++ and ...`. This works
-- because the text index injects its tokenizer into `hasPhrase` (previously `splitByRegexp` was excluded).

DROP TABLE IF EXISTS tab_phrase;

CREATE TABLE tab_phrase
(
    id UInt64,
    description String,
    INDEX idx description TYPE text(tokenizer = splitByRegexp('[^\\p{L}\\p{N}#+]+'), support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_phrase VALUES
    (1, 'we use C++ and go'),
    (2, 'built with C# and react'),
    (3, 'C is our language');

SELECT 'phrase: hasPhrase([C# and]) -> 2 (not row 1 with C++)';
SELECT id FROM tab_phrase WHERE hasPhrase(description, 'C# and') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT 'phrase: hasPhrase([C++ and]) -> 1';
SELECT id FROM tab_phrase WHERE hasPhrase(description, 'C++ and') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_phrase;

-- 6. `hasPhrase` on a `splitByRegexp` index combined with a postprocessor is explicitly rejected: the
-- index rewrite would otherwise assume whitespace-splitting and `splitByNonAlpha` tokens, giving wrong
-- results for a tokenizer that preserves `#`/`+`. Without a postprocessor the combination is supported
-- (section 5). Rejecting is safer than silently falling back to the default `splitByNonAlpha`.

DROP TABLE IF EXISTS tab_phrase_pp;

CREATE TABLE tab_phrase_pp
(
    id UInt64,
    description String,
    INDEX idx description TYPE text(tokenizer = splitByRegexp('[^\\p{L}\\p{N}#+]+'), postprocessor = lower(description))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_phrase_pp VALUES (1, 'we use C++ and go'), (2, 'built with C# and react');

SELECT id FROM tab_phrase_pp WHERE hasPhrase(description, 'C# and') SETTINGS use_skip_indexes = 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE tab_phrase_pp;
