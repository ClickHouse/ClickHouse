-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Negative tests';

CREATE TABLE tab
(
    id UInt32,
    col_str String,
    message String,
    arr Array(String),
    INDEX idx(`message`) TYPE text(tokenizer = 'default'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES (1, 'b', 'b', ['c']);

-- Must accept two arguments
SELECT id FROM tab WHERE searchAny(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT id FROM tab WHERE searchAny('a', 'b', 'c'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT id FROM tab WHERE searchAll(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT id FROM tab WHERE searchAll('a', 'b', 'c'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- 1st arg must be String or FixedString
SELECT id FROM tab WHERE searchAny(1, ['a']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id FROM tab WHERE searchAll(1, ['a']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- 2nd arg must be const Array(String)
SELECT id FROM tab WHERE searchAny(message, 'b'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id FROM tab WHERE searchAny(message, materialize('b')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id FROM tab WHERE searchAny(message, materialize(['b'])); -- { serverError ILLEGAL_COLUMN }
SELECT id FROM tab WHERE searchAll(message, 'b'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id FROM tab WHERE searchAll(message, materialize('b')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id FROM tab WHERE searchAll(message, materialize(['b'])); -- { serverError ILLEGAL_COLUMN }
-- search functions must be called on a column with text index
SELECT id FROM tab WHERE searchAny('a', ['b']); -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE searchAny(col_str, ['b']); -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE searchAll('a', ['b']); -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE searchAll(col_str, ['b']); -- { serverError BAD_ARGUMENTS }
-- search function supports a max of 64 needles
SELECT id FROM tab WHERE searchAny(message, ['a b c d e f g h i j k l m n o p q r s t u v w x y z a b c d e f g h i j k l m n o p q r s t u v w x y z a b c d e f g h i j k l m']); -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE searchAny(message, ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm']); -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;

SELECT 'Default tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'default'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, message)
VALUES
    (1, 'abc+ def- foo!'),
    (2, 'abc+ def- bar?'),
    (3, 'abc+ baz- foo!'),
    (4, 'abc+ baz- bar?'),
    (5, 'abc+ zzz- foo!'),
    (6, 'abc+ zzz- bar?');

SELECT groupArray(id) FROM tab WHERE searchAny(message, ['abc']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['ab']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['foo']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['bar']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['abc foo!']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['abc bar?']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['foo bar']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['foo ba']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['fo ba']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['abc', 'foo!']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['abc', 'bar?']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['foo', 'bar']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['foo', 'ba']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['fo', 'ba']);

SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abc']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['ab']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['foo']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['bar']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abc foo!']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abc bar?']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['foo bar']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abc fo']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abc', 'foo!']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abc', 'bar?']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['foo', 'bar']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abc', 'fo']);

DROP TABLE tab;

SELECT 'Ngram tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'ngram', ngram_size = 4),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab
VALUES
(1, 'abcdef'),
(2, 'bcdefg'),
(3, 'cdefgh'),
(4, 'defghi'),
(5, 'efghij');

SELECT groupArray(id) FROM tab WHERE searchAny(message, ['efgh']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['efg']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['cdef']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['defg']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['cdef', 'defg']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['efgh', 'cdef', 'defg']);

SELECT groupArray(id) FROM tab WHERE searchAll(message, ['efgh']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['efg']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['cdef']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['defg']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['cdef', 'defg']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['efgh', 'cdef', 'defg']);

DROP TABLE tab;

SELECT 'Split tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'split', separators = ['()', '\\']),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab
VALUES
(1, '  a  bc d'),
(2, '()()a()bc()d'),
(3, ',()a(),bc,(),d,'),
(4, '\\a\n\\bc\\d\n'),
(5, '\na\n\\bc\\d\\');

SELECT groupArray(id) FROM tab WHERE searchAny(message, ['a']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['bc']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['d']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['a', 'bc']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['a', 'd']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['bc', 'd']);

SELECT groupArray(id) FROM tab WHERE searchAll(message, ['a']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['bc']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['d']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['a', 'bc']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['a', 'd']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['bc', 'd']);

DROP TABLE tab;

SELECT 'NoOp tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'no_op'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab
VALUES
(1, 'abc def'),
(2, 'abc fgh'),
(3, 'def efg'),
(4, 'abcdef');

SELECT groupArray(id) FROM tab WHERE searchAny(message, ['abc']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['def']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['abc', 'def']);
SELECT groupArray(id) FROM tab WHERE searchAny(message, ['abcdef']);

SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abc']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['def']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abc', 'def']);
SELECT groupArray(id) FROM tab WHERE searchAll(message, ['abcdef']);

DROP TABLE tab;

SELECT 'Text index analysis';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'default') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT number, 'Hello, ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello, World' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo, ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHouse is the fast, really fast!' FROM numbers(1024);

SELECT 'searchAny is used during index analysis';

SELECT 'Text index should choose none for non-existent term';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Click'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3; -- Skip the primary index parts and granules.

SELECT 'Text index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hallo'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hallo Word']) -- Word does not exist in terms
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hallo', 'Word']) -- Word does not exist in terms
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hello Word'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hello', 'Word'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hallo', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hello', 'Hallo'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['ClickHouse'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose all 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['ClickHouse World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose all 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['ClickHouse', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'searchAll is used during index analysis';

SELECT 'Text index should choose none for non-existent term';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Click'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hallo'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hello World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hello', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none if any term does not exists in dictionary';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hallo Word']) -- Word does not exist in terms
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none if any term does not exists in dictionary';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hallo', 'Word']) -- Word does not exist in terms
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hello'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hallo World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hallo', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hello Hallo'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hello', 'Hallo'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['ClickHouse'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['ClickHouse World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['ClickHouse', 'World'])
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
    INDEX idx(`message`) TYPE text(tokenizer = 'default') GRANULARITY 1
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
        WHEN modulo(number, 4) = 3 THEN 'ClickHouse is the fast, really fast!'
    END
FROM numbers(1024);

SELECT 'Text index should choose 50% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hello World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 50% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hello', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose all granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hello ClickHouse'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose all granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAny(message, ['Hello', 'ClickHouse'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 25% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hello World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 25% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hello', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 25% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hello World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 25% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE searchAll(message, ['Hello', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;
