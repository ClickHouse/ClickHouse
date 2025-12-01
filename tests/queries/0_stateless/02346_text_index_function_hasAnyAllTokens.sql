-- Tags: no-parallel-replicas, long

SET enable_analyzer = 1;
SET use_query_condition_cache = 0;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Negative tests';

CREATE TABLE tab
(
    id UInt32,
    col_str String,
    message String,
    arr Array(String),
    INDEX idx(`message`) TYPE text(tokenizer = 'splitByNonAlpha'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES (1, 'b', 'b', ['c']), (2, 'c', 'c', ['c']), (3, '', '', ['']);

-- Must accept two arguments
SELECT id FROM tab WHERE hasAnyTokens(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT id FROM tab WHERE hasAnyTokens('a', 'b', 'c'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT id FROM tab WHERE hasAllTokens(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT id FROM tab WHERE hasAllTokens('a', 'b', 'c'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- 1st arg must be String or FixedString
SELECT id FROM tab WHERE hasAnyTokens(1, ['a']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id FROM tab WHERE hasAllTokens(1, ['a']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- 2nd arg must be const const String or const Array(String)
SELECT id FROM tab WHERE hasAnyTokens(message, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id FROM tab WHERE hasAnyTokens(message, materialize('b')); -- { serverError ILLEGAL_COLUMN }
SELECT id FROM tab WHERE hasAnyTokens(message, materialize(['b'])); -- { serverError ILLEGAL_COLUMN }
SELECT id FROM tab WHERE hasAllTokens(message, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id FROM tab WHERE hasAllTokens(message, materialize('b')); -- { serverError ILLEGAL_COLUMN }
SELECT id FROM tab WHERE hasAllTokens(message, materialize(['b'])); -- { serverError ILLEGAL_COLUMN }
-- Supports a max of 64 needles
SELECT id FROM tab WHERE hasAnyTokens(message, ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'aa', 'bb', 'cc', 'dd', 'ee', 'ff', 'gg', 'hh', 'ii', 'jj', 'kk', 'll', 'mm', 'nn', 'oo', 'pp', 'qq', 'rr', 'ss', 'tt', 'uu', 'vv', 'ww', 'xx', 'yy', 'zz', 'aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff', 'ggg', 'hhh', 'iii', 'jjj', 'kkk', 'lll', 'mmm']); -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE hasAnyTokens(message, 'a b c d e f g h i j k l m n o p q r s t u v w x y z aa bb cc dd ee ff gg hh ii jj kk ll mm nn oo pp qq rr ss tt uu vv ww xx yy zz aaa bbb ccc ddd eee fff ggg hhh iii jjj kkk lll mmm'); -- { serverError BAD_ARGUMENTS }

SELECT 'Test singular aliases';

SELECT hasAnyToken('a b', 'b') FORMAT Null;
SELECT hasAnyToken('a b', ['b']) FORMAT Null;
SELECT hasAllToken('a b', 'b') FORMAT Null;
SELECT hasAllToken('a b', ['b']) FORMAT Null;

SELECT 'Test what happens when hasAnyTokens/All is called on a column without index';

-- We expected that the default tokenizer is used
-- { echoOn }
SELECT hasAnyTokens('a b', ['b']);
SELECT hasAnyTokens('a b', ['c']);
SELECT hasAnyTokens('a b', 'b');
SELECT hasAnyTokens('a b', 'c');
SELECT hasAnyTokens(materialize('a b'), ['b']);
SELECT hasAnyTokens(materialize('a b'), ['c']);
SELECT hasAnyTokens(materialize('a b'), 'b');
SELECT hasAnyTokens(materialize('a b'), 'c');
--
SELECT hasAllTokens('a b', ['a', 'b']);
SELECT hasAllTokens('a b', ['a', 'c']);
SELECT hasAllTokens('a b', 'a b');
SELECT hasAllTokens('a b', 'a c');
SELECT hasAllTokens(materialize('a b'), ['a', 'b']);
SELECT hasAllTokens(materialize('a b'), ['a', 'c']);
SELECT hasAllTokens(materialize('a b'), 'a b');
SELECT hasAllTokens(materialize('a b'), 'a c');

-- These are equivalent to the lines above, but using Search{Any,All} in the filter step.
-- We keep this test because the direct read optimization substituted Search{Any,All} only
-- when they are in the filterStep, and we want to detect any variation eagerly.
SELECT id FROM tab WHERE hasAnyTokens('a b', ['b']);
SELECT id FROM tab WHERE hasAnyTokens('a b', ['c']);
SELECT id FROM tab WHERE hasAnyTokens(col_str, ['b']);
SELECT id FROM tab WHERE hasAnyTokens(col_str, ['c']);

SELECT id FROM tab WHERE hasAnyTokens('a b', 'b');
SELECT id FROM tab WHERE hasAnyTokens('a b', 'c');
SELECT id FROM tab WHERE hasAnyTokens(col_str, 'b');
SELECT id FROM tab WHERE hasAnyTokens(col_str, 'c');

SELECT id FROM tab WHERE hasAllTokens('a b', ['a b']);
SELECT id FROM tab WHERE hasAllTokens('a b', ['a c']);
SELECT id FROM tab WHERE hasAllTokens(col_str, ['a b']);
SELECT id FROM tab WHERE hasAllTokens(col_str, ['a c']);

SELECT id FROM tab WHERE hasAllTokens('a b', 'a b');
SELECT id FROM tab WHERE hasAllTokens('a b', 'a c');
SELECT id FROM tab WHERE hasAllTokens(col_str, 'a a');
SELECT id FROM tab WHERE hasAllTokens(col_str, 'b c');

-- Test search without needle on non-empty columns (all are expected to match nothing)
SELECT count() FROM tab WHERE hasAnyTokens(col_str, []);
SELECT count() FROM tab WHERE hasAllTokens(col_str, []);
SELECT count() FROM tab WHERE hasAnyTokens(col_str, ['']);
SELECT count() FROM tab WHERE hasAnyTokens(col_str, '');
SELECT count() FROM tab WHERE hasAnyTokens(col_str, ['','']);
-- { echoOff }

DROP TABLE tab;

-- Test specifically FixedString columns without text index
CREATE TABLE tab
(
    id UInt8,
    s FixedString(11)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'goodbye'), (3, 'hello moon');

-- { echoOn }
SELECT id FROM tab WHERE hasAnyTokens(s, ['hello']) ORDER BY id;
SELECT id FROM tab WHERE hasAnyTokens(s, ['moon', 'goodbye']) ORDER BY id;
SELECT id FROM tab WHERE hasAnyTokens(s, ['unknown', 'goodbye']) ORDER BY id;

SELECT id FROM tab WHERE hasAllTokens(s, ['hello', 'world']) ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(s, ['goodbye']) ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(s, ['hello', 'moon']) ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(s, ['hello', 'unknown']) ORDER BY id;
-- { echoOff }

DROP TABLE tab;

SELECT 'FixedString input columns';

CREATE TABLE tab (
    id Int,
    text FixedString(16),
    INDEX idx_text(text) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE=MergeTree()
ORDER BY (id);

INSERT INTO tab VALUES(1, toFixedString('bar', 3)), (2, toFixedString('foo', 3));

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(text, ['bar']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(text, ['bar']);

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(text, 'bar');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(text, 'bar');

DROP TABLE tab;

SELECT '-- Default tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'splitByNonAlpha'),
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

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['abc']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['ab']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['foo']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['bar']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['abc', 'foo']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['abc', 'bar']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['foo', 'bar']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['foo', 'ba']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['fo', 'ba']);

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'ab+');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'foo-');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'abc+* foo+');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'fo ba');

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['abc']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['ab']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['bar']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['abc', 'foo']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['abc', 'bar']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['foo', 'bar']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['abc', 'fo']);

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'ab+');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'foo-');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'abc+* foo+');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'abc ba');

DROP TABLE tab;

SELECT '-- Ngram tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = ngrams(4)),
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

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['efgh']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['efg']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['cdef']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['defg']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['cdef', 'defg']); -- search cdefg
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['efgh', 'cdef', 'defg']); --search for either cdefg or defgh

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'efgh');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'efg');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'efghi');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'cdefg');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'cdefgh');

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['efgh']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['efg']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['cdef']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['defg']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['cdef', 'defg']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['efgh', 'cdef', 'defg']);

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'efgh');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'efg');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'efghi');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'cdefg');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'cdefgh');

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

INSERT INTO tab
VALUES
(1, '  a  bc d'),
(2, '()()a()bc()d'),
(3, ',()a(),bc,(),d,'),
(4, '\\a\n\\bc\\d\n'),
(5, '\na\n\\bc\\d\\');

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['a']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['bc']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['d']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['a', 'bc']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['a', 'd']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['bc', 'd']);

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'a*');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'bc((');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'd()');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'a\\bc');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'a d');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, '\\,bc,()');

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['a']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['bc']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['d']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['a', 'bc']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['a', 'd']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['bc', 'd']);

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'a*');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'bc((');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'd()');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'a\\bc');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'a d');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'a\\,bc,()');

DROP TABLE tab;

SELECT '-- NoOp tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'array'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab
VALUES
(1, 'abc def'),
(2, 'abc fgh'),
(3, 'def efg'),
(4, 'abcdef');

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['abc']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['def']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['abc', 'def']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['abcdef']);

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'abc');
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, 'abc def');

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['abc']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['def']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['abc', 'def']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['abcdef']);

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'abc');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'abc def');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, 'abcdef ');

DROP TABLE tab;

SELECT 'Duplicate tokens';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'splitByNonAlpha'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab VALUES
    (1, 'hello world'),
    (2, 'hello world, hello everyone');

SELECT count() FROM tab WHERE hasAnyTokens(message, ['hello']);
SELECT count() FROM tab WHERE hasAnyTokens(message, ['hello', 'hello']);
SELECT count() FROM tab WHERE hasAnyTokens(message, 'hello hello');

SELECT count() FROM tab WHERE hasAllTokens(message, ['hello']);
SELECT count() FROM tab WHERE hasAllTokens(message, ['hello', 'hello']);
SELECT count() FROM tab WHERE hasAllTokens(message, 'hello hello');

DROP TABLE tab;

SELECT 'Combination with the tokens function';

SELECT '-- Default tokenizer';
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = 'splitByNonAlpha'),
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

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('abc', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('ab', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('foo', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('bar', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('abc foo', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('abc bar', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('foo bar', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('foo ba', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('fo ba', 'splitByNonAlpha'));

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('abc', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('ab', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('foo', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('bar', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('abc foo', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('abc bar', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('foo bar', 'splitByNonAlpha'));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('abc fo', 'splitByNonAlpha'));

DROP TABLE tab;

SELECT '-- Ngram tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = ngrams(4)),
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

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('efgh', 'ngrams', 4));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('efg', 'ngrams', 4));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('cdef', 'ngrams', 4));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('defg', 'ngrams', 4));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('cdefg', 'ngrams', 4)); -- search cdefg
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, arrayConcat(tokens('cdefg', 'ngrams', 4), tokens('defgh', 'ngrams', 4))); --search for either cdefg or defgh

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('efgh', 'ngrams', 4));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('efg', 'ngrams', 4));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('cdef', 'ngrams', 4));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('defg', 'ngrams', 4));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('cdefg', 'ngrams', 4));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, arrayConcat(tokens('cdefg', 'ngrams', 4), tokens('defgh', 'ngrams', 4)));

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

INSERT INTO tab
VALUES
(1, '  a  bc d'),
(2, '()()a()bc()d'),
(3, ',()a(),bc,(),d,'),
(4, '\\a\n\\bc\\d\n'),
(5, '\na\n\\bc\\d\\');

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('a', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('bc', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('d', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('a()bc', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('a\\d', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('bc\\d', 'splitByString', ['()', '\\']));

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('a', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('bc', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('d', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('a()bc', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('a\\d', 'splitByString', ['()', '\\']));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('bc\\d', 'splitByString', ['()', '\\']));

DROP TABLE tab;

SELECT '-- NoOp tokenizer';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'array'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab
VALUES
(1, 'abc def'),
(2, 'abc fgh'),
(3, 'def efg'),
(4, 'abcdef');

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('abc', 'array'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('def', 'array'));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, arrayConcat(tokens('def', 'array'), tokens('def', 'array')));
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, tokens('abcdef', 'array'));

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('abc', 'array'));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('def', 'array'));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, arrayConcat(tokens('def', 'array'), tokens('def', 'array')));
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, tokens('abcdef', 'array'));

DROP TABLE tab;

SELECT 'Text index analysis';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT number, 'Hello, ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello, World' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo, ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHouse is fast, really fast!' FROM numbers(1024);

SELECT 'hasAnyTokens is used during index analysis';

SELECT 'Text index overload 1 should choose none for non-existent term';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['Click'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3; -- Skip the primary index parts and granules.

SELECT 'Text index overload 2 should choose none for non-existent term';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, 'Click')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3; -- Skip the primary index parts and granules.

SELECT 'Text index overload 1 should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['Hallo'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 2 should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, 'Hallo')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 1 should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['Hallo', 'Word']) -- Word does not exist in terms
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 2 should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, 'Hallo Word') -- Word does not exist in terms
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['Hello', 'Word'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['Hallo', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['Hello', 'Hallo'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['ClickHouse'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose all 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['ClickHouse', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'hasAllTokens is used during index analysis';

SELECT 'Text index overload 1 should choose none for non-existent term';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['Click'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 2 should choose none for non-existent term';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, 'Click')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 1 should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['Hallo'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 2 should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, 'Hallo')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 1 should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['Hello', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 2 should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, 'Hello World')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 1 should choose none if any term does not exists in dictionary';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['Hallo', 'Word']) -- Word does not exist in terms
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index overload 2 should choose none if any term does not exists in dictionary';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, 'Hallo Word') -- Word does not exist in terms
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 2 parts and 2048 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['Hello'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['Hallo', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['Hello', 'Hallo'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['ClickHouse'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['ClickHouse', 'World'])
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
    INDEX idx(`message`) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
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
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['Hello', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose all granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAnyTokens(message, ['Hello', 'ClickHouse'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT 'Text index should choose 25% of granules';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE hasAllTokens(message, ['Hello', 'World'])
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt8,
    s FixedString(11)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'goodbye'), (3, 'hello moon');

SELECT 'Test hasAnyTokens and hasAllTokens on a non-indexed FixedString column';

-- { echoOn }
SELECT id FROM tab WHERE hasAnyTokens(s, ['hello']) ORDER BY id;
SELECT id FROM tab WHERE hasAnyTokens(s, ['moon', 'goodbye']) ORDER BY id;
SELECT id FROM tab WHERE hasAnyTokens(s, ['unknown', 'goodbye']) ORDER BY id;

SELECT id FROM tab WHERE hasAnyTokens(s, 'hello') ORDER BY id;
SELECT id FROM tab WHERE hasAnyTokens(s, 'moon goodbye') ORDER BY id;
SELECT id FROM tab WHERE hasAnyTokens(s, 'unknown goodbye') ORDER BY id;

SELECT id FROM tab WHERE hasAllTokens(s, ['hello', 'world']) ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(s, ['goodbye']) ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(s, ['hello', 'moon']) ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(s, ['hello', 'unknown']) ORDER BY id;

SELECT id FROM tab WHERE hasAllTokens(s, 'hello world') ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(s, 'goodbye') ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(s, 'hello moon') ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(s, 'hello unknown') ORDER BY id;
-- { echoOff }

DROP TABLE IF EXISTS tab;
