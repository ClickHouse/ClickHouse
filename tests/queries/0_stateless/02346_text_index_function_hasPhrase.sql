-- Tags: no-parallel-replicas

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

SELECT '-- Default tokenizer';

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

SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc def');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'def foo');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'def bar');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc baz');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'baz foo');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'baz bar');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc zzz');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'zzz foo');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'zzz bar');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc def foo');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc def bar');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc baz foo');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc baz bar');
-- no match: wrong order
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'def abc');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'foo def');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'bar def');
-- no match: not consecutive
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc foo');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc bar');
-- separator characters in phrase are removed before matching
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc+ def-');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'abc...def');

DROP TABLE tab;

SELECT '-- Ngram tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = ngrams(3)),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES
    (1, 'the quick brown fox'),
    (2, 'quick brown dog'),
    (3, 'brown fox jumps'),
    (4, 'lazy dog sleeps'),
    (5, 'the lazy fox');

SELECT '---- ngrams turn a substring search into a token phrase search via the text index';
-- trimming characters from either side still matches (substring property)
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'uick brow', 'ngrams(3)');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'rown fo', 'ngrams(3)');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'azy do', 'ngrams(3)');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'he quic', 'ngrams(3)');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'uick brown fo', 'ngrams(3)');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'ox jump', 'ngrams(3)');
-- no match
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'rown quic', 'ngrams(3)');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'issing tex', 'ngrams(3)');

DROP TABLE tab;

SELECT '-- Split tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = splitByString(['()', '\\'])),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES
    (1, '()a()bc()d()'),
    (2, '()d()bc()a()'),
    (3, '()a()d()'),
    (4, '\\a\\bc\\d\\'),
    (5, '()a()bc()');

SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'a()bc', 'splitByString([\'()\', \'\\\\\'])');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'bc()d', 'splitByString([\'()\', \'\\\\\'])');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'a()d', 'splitByString([\'()\', \'\\\\\'])');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'a()bc()d', 'splitByString([\'()\', \'\\\\\'])');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'd()bc', 'splitByString([\'()\', \'\\\\\'])');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'bc()a', 'splitByString([\'()\', \'\\\\\'])');
-- separator characters in phrase are removed before matching
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'a::::bc', 'splitByString([\'()\', \'\\\\\'])');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'a\\\\bc', 'splitByString([\'()\', \'\\\\\'])');

DROP TABLE tab;

SELECT '-- FixedString input columns';

CREATE TABLE tab
(
    id UInt32,
    text FixedString(16),
    INDEX idx_text(text) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO tab VALUES
    (1, toFixedString('quick brown fox', 15)),
    (2, toFixedString('brown quick fox', 15)),
    (3, toFixedString('quick fox', 9));

SELECT groupArray(id) FROM tab WHERE hasPhrase(text, 'quick brown');
SELECT groupArray(id) FROM tab WHERE hasPhrase(text, 'brown quick');
SELECT groupArray(id) FROM tab WHERE hasPhrase(text, 'quick fox');

DROP TABLE tab;

SELECT '-- asciiCJK tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = asciiCJK),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES
    (1, 'hello错误502需要处理kitty'),
    (2, 'taichi张三丰in the house'),
    (3, 'hello world'),
    (4, '错误502需要');

-- consecutive tokens
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, '错误502', 'asciiCJK');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, '需要处理', 'asciiCJK');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, '三丰in', 'asciiCJK');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'hello world', 'asciiCJK');
-- wrong order
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, '502错误', 'asciiCJK');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, 'in三', 'asciiCJK');
-- not consecutive
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, '错处', 'asciiCJK');
SELECT groupArray(id) FROM tab WHERE hasPhrase(message, '张in', 'asciiCJK');

DROP TABLE tab;

SELECT 'Text index analysis';

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

SELECT 'Text index should choose none for non-existent phrase';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'Click House')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'Hello World')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'Hallo ClickHouse')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab
    WHERE hasPhrase(message, 'Hello ClickHouse') OR hasPhrase(message, 'Hallo ClickHouse')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none if any term does not exist in dictionary';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'Hallo Word')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 1 part and 1024 granules (hint mode) for existing tokens but they are in a wrong order';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'ClickHouse Hello')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;

SELECT 'Chooses mixed granules inside part';

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

INSERT INTO tab
SELECT
    number,
    CASE
        WHEN modulo(number, 4) = 0 THEN 'Hello, ClickHouse'
        WHEN modulo(number, 4) = 1 THEN 'Hello, World'
        WHEN modulo(number, 4) = 2 THEN 'Hallo, ClickHouse'
        WHEN modulo(number, 4) = 3 THEN 'ClickHouse is fast, really fast!'
    END
FROM numbers(1024);

SELECT 'Text index should choose 25% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'Hello World')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 25% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'Hello ClickHouse')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 25% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'Hallo ClickHouse')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 50% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab
    WHERE hasPhrase(message, 'Hello ClickHouse') OR hasPhrase(message, 'Hallo ClickHouse')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'hasPhrase with 3rd argument bypasses the index';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'Hello World', 'splitByNonAlpha')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;

SELECT 'NOT hasPhrase - Text index analysis';

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

SELECT 'NOT hasPhrase should choose all parts and granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE NOT hasPhrase(message, 'Hello World')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'NOT hasPhrase should choose all parts even for phrases with no matching tokens';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE NOT hasPhrase(message, 'Click House')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'AND NOT hasPhrase should reduce to parts containing the positive phrase';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasPhrase(message, 'Hello World') AND NOT hasPhrase(message, 'Hallo ClickHouse')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;
