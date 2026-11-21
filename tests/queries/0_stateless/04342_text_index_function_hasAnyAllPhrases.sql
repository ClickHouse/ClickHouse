-- Tags: no-parallel-replicas

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

SELECT '-- Functional correctness with a text index (default tokenizer)';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = splitByNonAlpha),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, message) VALUES
    (1, 'abc+ def- foo!'),
    (2, 'abc+ def- bar?'),
    (3, 'abc+ baz- foo!'),
    (4, 'abc+ baz- bar?'),
    (5, 'abc+ zzz- foo!'),
    (6, 'abc+ zzz- bar?');

SELECT '---- hasAnyPhrases';
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['abc def', 'baz bar']);
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['zzz foo', 'def bar']);
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['missing', 'absent']);
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['def abc', 'abc foo']); -- wrong order / not consecutive
SELECT '---- hasAllPhrases';
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, ['abc def', 'def foo']);
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, ['abc baz', 'baz bar']);
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, ['abc def', 'missing']);
SELECT '---- empty / degenerate phrases';
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, []);
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, []);
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['', 'abc def']);
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, ['abc def', '']);

SELECT '---- result is identical to the OR/AND of hasPhrase (expect no rows)';
SELECT id FROM tab
WHERE hasAnyPhrases(message, ['abc def', 'baz bar']) != (hasPhrase(message, 'abc def') OR hasPhrase(message, 'baz bar'))
   OR hasAllPhrases(message, ['abc def', 'def foo']) != (hasPhrase(message, 'abc def') AND hasPhrase(message, 'def foo'))
ORDER BY id;

DROP TABLE tab;

SELECT '-- Text index granule pruning';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT number, 'Hello, ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello, World' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo, ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHouse is fast, really fast!' FROM numbers(1024);

SELECT 'hasAnyPhrases: 2 parts and 2048 granules (one all-tokens query per phrase, OR-combined)';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyPhrases(message, ['Hello ClickHouse', 'Hallo ClickHouse'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'hasAllPhrases: prunes to the part with all tokens of both phrases (1 part, 1024 granules)';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllPhrases(message, ['Hello World', 'World Hello'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'hasAnyPhrases with a non-existent token in one phrase still keeps the other (1 part, 1024 granules)';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyPhrases(message, ['Hello World', 'NoSuchToken Here'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'hasAllPhrases with a non-existent token prunes everything (0 parts)';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllPhrases(message, ['Hello World', 'NoSuchToken'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'hasAnyPhrases/hasAllPhrases with a 3rd tokenizer argument bypasses the index (4 parts)';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyPhrases(message, ['Hello World'], 'splitByNonAlpha')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;

SELECT '-- Other tokenizers, functional';

SELECT '---- ngrams';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(`message`) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY (id);
INSERT INTO tab VALUES (1, 'the quick brown fox'), (2, 'quick brown dog'), (3, 'brown fox jumps'), (4, 'lazy dog sleeps'), (5, 'the lazy fox');
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['uick brow', 'azy do'], 'ngrams(3)');
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, ['uick brow', 'rown fo'], 'ngrams(3)');
DROP TABLE tab;

SELECT '---- splitByString';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(`message`) TYPE text(tokenizer = splitByString(['()', '\\'])))
ENGINE = MergeTree ORDER BY (id);
INSERT INTO tab VALUES (1, '()a()bc()d()'), (2, '()d()bc()a()'), (3, '()a()d()'), (4, '\\a\\bc\\d\\'), (5, '()a()bc()');
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['a()bc', 'd()bc'], 'splitByString([\'()\', \'\\\\\'])');
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, ['a()bc', 'bc()d'], 'splitByString([\'()\', \'\\\\\'])');
DROP TABLE tab;

SELECT '---- asciiCJK';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(`message`) TYPE text(tokenizer = asciiCJK))
ENGINE = MergeTree ORDER BY (id);
INSERT INTO tab VALUES (1, 'hello错误502需要处理kitty'), (2, 'taichi张三丰in the house'), (3, 'hello world'), (4, '错误502需要');
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['错误502', 'hello world'], 'asciiCJK');
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, ['错误502', '需要处理'], 'asciiCJK');
DROP TABLE tab;

SELECT '-- regression: index with a preprocessor (lower) applies it to column AND phrases on the index path';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(message)))
ENGINE = MergeTree ORDER BY (id);
INSERT INTO tab VALUES (1, 'Foo BaR Baz'), (2, 'Quick Brown Fox'), (3, 'no match here');
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['fOO bAR', 'BROWN fox']);
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, ['foo bar', 'bar baz']);
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['nope']);
SELECT '---- the hasPhrase OR rewrite agrees with the un-rewritten query (both case-insensitive via the index)';
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'fOO bAR') OR hasPhrase(message, 'BROWN fox') SETTINGS optimize_rewrite_has_phrase_or_chain = 1;
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'fOO bAR') OR hasPhrase(message, 'BROWN fox') SETTINGS optimize_rewrite_has_phrase_or_chain = 0;
DROP TABLE tab;

SELECT '-- regression: index with a non-default tokenizer (ngrams) is used by the 2-arg functions on the index path';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY (id);
INSERT INTO tab VALUES (1, 'abcdef'), (2, 'uvwxyz'), (3, 'nomatch');
SELECT groupArray(id) FROM tab WHERE hasAnyPhrases(message, ['bcd', 'wxy']);
SELECT groupArray(id) FROM tab WHERE hasAllPhrases(message, ['bcd', 'cde']);
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'bcd') OR hasPhrase(message, 'wxy') SETTINGS optimize_rewrite_has_phrase_or_chain = 1;
DROP TABLE tab;
