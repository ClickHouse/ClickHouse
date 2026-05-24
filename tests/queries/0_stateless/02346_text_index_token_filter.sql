-- Tests the token_filter parameter of text index.

DROP TABLE IF EXISTS tab;

SELECT 'Negative cases';

SELECT '-- token_filter must be a string constant (preset name) or an array of strings';

CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, token_filter = 42))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, token_filter = 1.5))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, token_filter = 'unknown_preset'))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, token_filter = [1, 2]))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, token_filter = concat('a', 'b')))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT 'splitByNonAlpha';
SELECT '-- english filter';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, token_filter = 'english')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'the quick brown fox'), (2, 'it was a lazy dog');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

DROP TABLE tab;

SELECT 'splitByNonAlpha';
SELECT '-- custom filter';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, token_filter = ['fox', 'dog'])
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'the quick brown fox'), (2, 'a lazy dog');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

DROP TABLE tab;

SELECT 'splitByString';
SELECT '-- english filter';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByString([' ']), token_filter = 'english')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'to be or not to be'), (2, 'brave and bold');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

DROP TABLE tab;

SELECT 'ngrams';
SELECT '-- custom filter';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = ngrams(3), token_filter = ['abc', 'def'])
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'abcdef');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

DROP TABLE tab;

SELECT 'sparseGrams';
SELECT '-- custom filter';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = sparseGrams(3), token_filter = ['abc'])
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'abc');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

DROP TABLE tab;

SELECT 'array';
SELECT '-- english filter';

CREATE TABLE tab
(
    id UInt64,
    arr Array(String),
    INDEX idx arr TYPE text(tokenizer = array, token_filter = 'english')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['the', 'quick', 'brown']), (2, ['it', 'was', 'lazy']);

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

DROP TABLE tab;

SELECT 'asciiCJK';
SELECT '-- english filter';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = asciiCJK, token_filter = 'english')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'the quick brown fox'), (2, 'it was a lazy dog');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

DROP TABLE tab;
