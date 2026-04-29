-- Testing min_token_chars and max_token_chars arguments of text index.

DROP TABLE IF EXISTS tab;

SELECT 'splitByNonAlpha';

SELECT '-- no limits';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'a bb ccc dddd eeeee');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT '-- min_token_chars = 3';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, min_token_chars = 3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'a bb ccc dddd eeeee');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT '-- max_token_chars = 3';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, max_token_chars = 3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'a bb ccc dddd eeeee');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT '-- min_token_chars = 2, max_token_chars = 4';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, min_token_chars = 2, max_token_chars = 4)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'a bb ccc dddd eeeee');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT 'splitByNonAlpha UTF-8';

SELECT '-- no limits';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'я до кот слон медведь');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT '-- min_token_chars = 3';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, min_token_chars = 3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'я до кот слон медведь');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT '-- max_token_chars = 3';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, max_token_chars = 3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'я до кот слон медведь');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT 'asciiCJK';

SELECT '-- no limits';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = asciiCJK)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'hello world 世界和平');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT '-- min_token_chars = 2 (filters single CJK chars)';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = asciiCJK, min_token_chars = 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'hello world 世界和平');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT 'splitByString';

SELECT '-- no limits';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByString([',']))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'a,bb,ccc,dddd');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT '-- min_token_chars = 3';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByString([',']), min_token_chars = 3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, 'a,bb,ccc,dddd');

SELECT token FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s) ORDER BY token;

DROP TABLE tab;

SELECT 'Validation';

SELECT '-- min > max';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, min_token_chars = 5, max_token_chars = 2)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT '-- ngrams with min_token_chars';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = ngrams(3), min_token_chars = 4)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT '-- ngrams with max_token_chars';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = ngrams(3), max_token_chars = 4)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT '-- sparseGrams with min_token_chars';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = sparseGrams, min_token_chars = 4)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT '-- sparseGrams with max_token_chars';

CREATE TABLE tab (id UInt64, s String, INDEX idx_s (s) TYPE text(tokenizer = sparseGrams, max_token_chars = 4)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS tab;
