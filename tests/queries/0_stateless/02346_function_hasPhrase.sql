SELECT 'Negative tests';
-- Must accept two to three arguments
SELECT hasPhrase(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasPhrase('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasPhrase('a', 'b', 'c', 'd'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- 1st arg must be String or FixedString
SELECT hasPhrase(1, 'hello'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- 2nd arg must be const String
SELECT hasPhrase('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasPhrase('a', materialize('b')); -- { serverError ILLEGAL_COLUMN }
-- 3rd arg (if given) must be const String (tokenizer name)
SELECT hasPhrase('a', 'b', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasPhrase('a', 'b', 'unsupported_tokenizer'); -- { serverError BAD_ARGUMENTS }
-- sparseGrams is not supported because gram ordering depends on context
SELECT hasPhrase('a', 'b', 'sparseGrams'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase('a', 'b', 'array'); -- { serverError BAD_ARGUMENTS }
-- NULL arguments
SELECT hasPhrase(NULL); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(NULL, NULL); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasPhrase(NULL, 'quick brown');
SELECT hasPhrase('the quick brown fox', NULL); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- 2nd arg must be const String, materialize(NULL) should not be accepted
SELECT hasPhrase('', materialize(NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Constants: hasPhrase should be constant';

SELECT 'Default tokenizer (splitByNonAlpha)';

SELECT hasPhrase('the quick brown fox jumps over the lazy dog', 'quick brown');
SELECT hasPhrase('the quick brown fox jumps over the lazy dog', 'quick brown fox');
SELECT hasPhrase('the quick brown fox jumps over the lazy dog', 'the lazy dog');
SELECT hasPhrase('the quick brown fox jumps over the lazy dog', 'the quick brown fox jumps over the lazy dog');
SELECT hasPhrase('the quick brown fox jumps over the lazy dog', 'quick fox');
SELECT hasPhrase('the quick brown fox jumps over the lazy dog', 'brown quick');
SELECT hasPhrase('the quick brown fox jumps over the lazy dog', 'dog lazy');
SELECT hasPhrase('the quick brown fox', 'quick');
SELECT hasPhrase('the quick brown fox', 'missing');
SELECT hasPhrase('the the the quick', 'the quick');
SELECT hasPhrase('the the the quick', 'the the');
SELECT hasPhrase('the the the quick', 'the the the');
SELECT hasPhrase('the the the quick', 'the the the quick');
SELECT hasPhrase('the the the quick', 'the the the the');
SELECT '-- correct failure handling';
SELECT hasPhrase('a a a b', 'a a b');
SELECT hasPhrase('a b a b a b c', 'a b a b c');
SELECT hasPhrase('x x x x y', 'x x y');
SELECT hasPhrase('hello world', 'hello world');
SELECT hasPhrase('hello world', 'hello');
SELECT hasPhrase('hello world', 'world');
SELECT hasPhrase('hello---world...foo', 'hello world');
SELECT hasPhrase('one,two;three!four', 'two three');
SELECT hasPhrase('hello world', '');
SELECT hasPhrase('', 'hello');
SELECT hasPhrase('', '');
SELECT hasPhrase('hello world', '!!!');
SELECT '-- tokenizer separators in phrase are removed before matching';
SELECT hasPhrase('error: connection refused', 'error---connection');
SELECT hasPhrase('error: connection refused', 'error:connection');
SELECT hasPhrase('one two three', 'one...two...three');
SELECT hasPhrase('one two three', 'one!@#two$%^three');
SELECT '-- FixedString input';
SELECT hasPhrase(toFixedString('the quick brown fox', 19), 'quick brown');
SELECT '-- non-const input';
SELECT hasPhrase(materialize('the quick brown fox'), 'quick brown');
SELECT '-- Nullable(String) input';
SELECT hasPhrase(toNullable('the quick brown fox'), 'quick brown');
SELECT hasPhrase(CAST(NULL AS Nullable(String)), 'quick brown');
SELECT '-- Nullable(FixedString) input';
SELECT hasPhrase(toNullable(toFixedString('the quick brown fox', 19)), 'quick brown');
SELECT hasPhrase(CAST(NULL AS Nullable(FixedString(19))), 'quick brown');

SELECT '-- Nullable(String) column values';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message Nullable(String)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES (1, 'the quick brown fox'), (2, NULL), (3, 'quick brown eyes'), (4, NULL), (5, 'no match here');

SELECT hasPhrase(message, 'quick brown') FROM tab ORDER BY id;

DROP TABLE tab;

SELECT '-- Nullable(FixedString) column values';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message Nullable(FixedString(50))) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES (1, 'the quick brown fox'), (2, NULL), (3, 'quick brown eyes'), (4, NULL), (5, 'no match here');

SELECT hasPhrase(message, 'quick brown') FROM tab ORDER BY id;

DROP TABLE tab;

SELECT 'splitByString tokenizer';

SELECT hasPhrase('one::two::three::four', 'two::three', 'splitByString([\'::\'])');
SELECT hasPhrase('one::two::three::four', 'two::four', 'splitByString([\'::\'])');
SELECT hasPhrase('()a()bc()d()', 'a()bc', 'splitByString([\'()\'])');
SELECT hasPhrase('()a()bc()d()', 'a()d', 'splitByString([\'()\'])');
SELECT '-- tokenizer separators in phrase are removed before matching';
SELECT hasPhrase('one::two::three::four', 'two::::three', 'splitByString([\'::\'])');
SELECT hasPhrase('one::two::three::four', 'two::::::three', 'splitByString([\'::\'])');

SELECT 'ngrams tokenizer';

SELECT hasPhrase('abcdef', 'bcd', 'ngrams(3)');
SELECT hasPhrase('abcdef', 'abc', 'ngrams(3)');
SELECT hasPhrase('abcdef', 'cde', 'ngrams(3)');

SELECT 'Column values: hasPhrase should be non-constant';

SELECT 'Default tokenizer (splitByNonAlpha)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, 'the quick brown fox jumps over the lazy dog'),
    (2, 'a fast red car drove past the old house'),
    (3, 'clickhouse is a fast analytical database'),
    (4, 'the brown quick fox'),
    (5, 'quick brown foxes are fast');

SELECT id FROM tab WHERE hasPhrase(message, 'quick brown') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, 'fast analytical database') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, 'brown quick') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, 'the lazy dog') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, 'missing phrase') ORDER BY id;

DROP TABLE tab;

SELECT 'splitByString tokenizer';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, '()a()bc()d()'),
    (2, '()d()bc()a()'),
    (3, '()a()d()');

SELECT id FROM tab WHERE hasPhrase(message, 'a()bc', 'splitByString([\'()\'])') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, 'bc()d', 'splitByString([\'()\'])') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, 'a()d', 'splitByString([\'()\'])') ORDER BY id;

DROP TABLE tab;

SELECT 'ngrams tokenizer';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, 'abcdef'),
    (2, 'ghijkl'),
    (3, 'abcxyz');

SELECT id FROM tab WHERE hasPhrase(message, 'abc', 'ngrams(3)') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, 'cde', 'ngrams(3)') ORDER BY id;

DROP TABLE tab;

SELECT 'matchPhrase alias';

SELECT hasPhrase('the quick brown fox', 'quick brown');
SELECT matchPhrase('the quick brown fox', 'quick brown');

SELECT 'asciiCJK tokenizer';

SELECT hasPhrase('错误503', '错误', 'asciiCJK');
SELECT hasPhrase('错误503', '误503', 'asciiCJK');
SELECT hasPhrase('错误503', '错503', 'asciiCJK');
SELECT hasPhrase('taichi张三丰in the house', '张三', 'asciiCJK');
SELECT hasPhrase('taichi张三丰in the house', '丰in', 'asciiCJK');
SELECT hasPhrase('taichi张三丰in the house', '张in', 'asciiCJK');

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, 'hello错误502需要处理kitty'),
    (2, 'taichi张三丰in the house'),
    (3, 'hello world'),
    (4, '错误502需要');

SELECT id FROM tab WHERE hasPhrase(message, '错误502', 'asciiCJK') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, '需要处理', 'asciiCJK') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, '三丰in', 'asciiCJK') ORDER BY id;
SELECT id FROM tab WHERE hasPhrase(message, 'hello world', 'asciiCJK') ORDER BY id;

DROP TABLE tab;

SELECT 'NOT hasPhrase';

SELECT NOT hasPhrase('the quick brown fox', 'quick brown');
SELECT NOT hasPhrase('the quick brown fox', 'brown quick');
SELECT NOT hasPhrase('the quick brown fox', 'missing phrase');
SELECT NOT hasPhrase('hello world', 'hello world');
SELECT NOT hasPhrase('', 'hello');

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, 'the quick brown fox'),
    (2, 'the slow brown turtle'),
    (3, 'quick brown dog');

SELECT id FROM tab WHERE NOT hasPhrase(message, 'quick brown') ORDER BY id;
SELECT id FROM tab WHERE NOT hasPhrase(message, 'brown') ORDER BY id;
SELECT id FROM tab WHERE NOT hasPhrase(message, 'missing phrase') ORDER BY id;

DROP TABLE tab;
