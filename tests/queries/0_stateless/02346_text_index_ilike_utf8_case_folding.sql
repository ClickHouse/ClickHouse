-- Tags: no-fasttest
-- no-fasttest: lowerUTF8/upperUTF8 require a build with ICU.

-- `ILIKE '%needle%'` folds case per code point with `Poco::Unicode::toLower`, which agrees neither with the
-- ICU full case mapping of `lowerUTF8`/`upperUTF8` nor with the ASCII-only matching of the dictionary scan.
-- The dictionary scan must not answer such a predicate. Every query below must return the same rows it
-- returns with `use_skip_indexes = 0`.

SET explain_query_plan_default = 'legacy';
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT 'lowerUTF8 preprocessor, U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE folds to i + U+0307';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lowerUTF8(message)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, concat('zzzz', char(0xC4, 0xB0), 'zzzz')), (2, 'zzzzizzzz'), (3, 'hello world');

SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzzi%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzzi%';

SELECT 'lowerUTF8 preprocessor, U+212A KELVIN SIGN folds to k';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lowerUTF8(message)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, concat('zzzz', char(0xE2, 0x84, 0xAA), 'zzzz')), (2, 'zzzzkzzzz'), (3, 'hello world');

SELECT groupArray(id) FROM tab WHERE message ILIKE '%kzzz%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE message ILIKE '%kzzz%';

SELECT 'upperUTF8 preprocessor, U+00DF LATIN SMALL LETTER SHARP S folds to SS';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = upperUTF8(message)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, concat('zzzz', char(0xC3, 0x9F), 'zzzz')), (2, 'zzzzsszzzz'), (3, 'hello world');

SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzss%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzss%';

SELECT 'upperUTF8 preprocessor, U+017F LATIN SMALL LETTER LONG S folds to S';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = upperUTF8(message)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, concat('ab', char(0xC5, 0xBF), 'oop zzz')), (2, 'ABSOOP zzz'), (3, 'hello world');

SELECT groupArray(id) FROM tab WHERE message ILIKE '%bsoo%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE message ILIKE '%bsoo%';

SELECT 'no preprocessor, U+212A KELVIN SIGN is a token separator but ILIKE reads it as k';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, concat('zzzz', char(0xE2, 0x84, 0xAA), 'zzzz')), (2, 'zzzkzzzz'), (3, 'hello world');

SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzzk%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzzk%';

SELECT 'lower preprocessor, same as above';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(message)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, concat('zzzz', char(0xE2, 0x84, 0xAA), 'zzzz')), (2, 'zzzkzzzz'), (3, 'hello world');

SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzzk%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzzk%';

SELECT 'upper preprocessor, needle folded to K by the preprocessor is rejected too';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = upper(message)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, concat('zzzz', char(0xE2, 0x84, 0xAA), 'zzzz')), (2, 'zzzkzzzz'), (3, 'hello world');

SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzzk%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE message ILIKE '%zzzk%';

SELECT 'array tokenizer, same needle restriction';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, tag String, INDEX idx(tag) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, concat('zzzz', char(0xE2, 0x84, 0xAA), 'zzzz')), (2, 'zzzkzzzz'), (3, 'hello world');

SELECT groupArray(id) FROM tab WHERE tag ILIKE '%zzzk%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE tag ILIKE '%zzzk%';
-- The `k` needle must not reach the dictionary scan, a needle without one still must.
SELECT countIf(explain LIKE '%Name: idx%') FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE tag ILIKE '%zzzk%');
SELECT countIf(explain LIKE '%Name: idx%') FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE tag ILIKE '%zzzz%');

SELECT 'Which predicates still reach the dictionary scan';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab SELECT number, 'Hello Monkey' FROM numbers(10);
INSERT INTO tab SELECT number, 'Bonjour Monde' FROM numbers(10);

-- No character of the needle is reachable by case folding, so the dictionary answers it and prunes one part.
SELECT 'ILIKE %monde%', countIf(explain LIKE '%Name: idx%'), countIf(explain LIKE '%Granules: 10/20%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE message ILIKE '%monde%');

-- `k` can be spelled with U+212A, which the dictionary never sees.
SELECT 'ILIKE %monkey%', countIf(explain LIKE '%Name: idx%') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE message ILIKE '%monkey%');

-- Case-sensitive LIKE folds nothing and keeps the optimization.
SELECT 'LIKE %Monkey%', countIf(explain LIKE '%Name: idx%') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE message LIKE '%Monkey%');

SELECT groupArray(DISTINCT message) FROM tab WHERE message ILIKE '%monkey%';
SELECT groupArray(DISTINCT message) FROM tab WHERE message LIKE '%Monkey%';

DROP TABLE tab;
