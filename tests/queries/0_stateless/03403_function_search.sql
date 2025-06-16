SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Negative tests';

CREATE TABLE tab
(
    id UInt32,
    col_str String,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'default'),
)
ENGINE = MergeTree
ORDER BY (id);

-- Must accept two to four arguments
SELECT id from tab where searchAny(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT id from tab where searchAny('a', 'b', 'c', 'd', 'e'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT id from tab where searchAll(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT id from tab where searchAll('a', 'b', 'c', 'd', 'e'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- 1st and 2nd arg must be String or FixedString
SELECT id from tab where searchAny('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id from tab where searchAny(1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id from tab where searchAny(1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id from tab where searchAll('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id from tab where searchAll(1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id from tab where searchAll(1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- 2nd arg must be const String
SELECT id from tab where searchAny(message, toFixedString('b', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id from tab where searchAny(message, materialize('b')); -- { serverError ILLEGAL_COLUMN }
SELECT id from tab where searchAll(message, toFixedString('b', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT id from tab where searchAll(message, materialize('b')); -- { serverError ILLEGAL_COLUMN }
-- search functions should be used with the index column
SELECT id from tab where searchAny('a', 'b'); -- { serverError BAD_ARGUMENTS }
-- SELECT id from tab where searchAny(col_str, 'b'); -- { serverError BAD_ARGUMENTS }
SELECT id from tab where searchAll('a', 'b'); -- { serverError BAD_ARGUMENTS }
-- SELECT id from tab where searchAll(col_str, 'b'); -- { serverError BAD_ARGUMENTS }

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

SELECT groupArray(id) from tab where searchAny(message, 'abc');
SELECT groupArray(id) from tab where searchAny(message, 'ab');
SELECT groupArray(id) from tab where searchAny(message, 'foo');
SELECT groupArray(id) from tab where searchAny(message, 'bar');
SELECT groupArray(id) from tab where searchAny(message, 'abc foo!');
SELECT groupArray(id) from tab where searchAny(message, 'abc bar?');
SELECT groupArray(id) from tab where searchAny(message, 'foo bar');
SELECT groupArray(id) from tab where searchAny(message, 'foo ba');
SELECT groupArray(id) from tab where searchAny(message, 'fo ba');

SELECT groupArray(id) from tab where searchAll(message, 'abc');
SELECT groupArray(id) from tab where searchAny(message, 'ab');
SELECT groupArray(id) from tab where searchAll(message, 'foo');
SELECT groupArray(id) from tab where searchAll(message, 'bar');
SELECT groupArray(id) from tab where searchAll(message, 'abc foo!');
SELECT groupArray(id) from tab where searchAll(message, 'abc bar?');
SELECT groupArray(id) from tab where searchAll(message, 'foo bar');
SELECT groupArray(id) from tab where searchAll(message, 'abc fo');

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

SELECT groupArray(id) from tab where searchAny(message, 'efgh');
SELECT groupArray(id) from tab where searchAny(message, 'efg');
SELECT groupArray(id) from tab where searchAny(message, 'cdef');
SELECT groupArray(id) from tab where searchAny(message, 'defg');
SELECT groupArray(id) from tab where searchAny(message, 'cdef defg');
SELECT groupArray(id) from tab where searchAny(message, 'efgh cdef defg');

SELECT groupArray(id) from tab where searchAll(message, 'efgh');
SELECT groupArray(id) from tab where searchAll(message, 'efg');
SELECT groupArray(id) from tab where searchAll(message, 'cdef');
SELECT groupArray(id) from tab where searchAll(message, 'defg');
SELECT groupArray(id) from tab where searchAll(message, 'cdef defg');
SELECT groupArray(id) from tab where searchAll(message, 'efgh cdef defg');

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

SELECT groupArray(id) from tab where searchAny(message, 'a');
SELECT groupArray(id) from tab where searchAny(message, 'bc');
SELECT groupArray(id) from tab where searchAny(message, 'd');
SELECT groupArray(id) from tab where searchAny(message, 'a bc');
SELECT groupArray(id) from tab where searchAny(message, 'a d');
SELECT groupArray(id) from tab where searchAny(message, 'bc d');

SELECT groupArray(id) from tab where searchAll(message, 'a');
SELECT groupArray(id) from tab where searchAll(message, 'bc');
SELECT groupArray(id) from tab where searchAll(message, 'd');
SELECT groupArray(id) from tab where searchAll(message, 'a bc');
SELECT groupArray(id) from tab where searchAll(message, 'a d');
SELECT groupArray(id) from tab where searchAll(message, 'bc d');

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

SELECT groupArray(id) from tab where searchAny(message, 'abc');
SELECT groupArray(id) from tab where searchAny(message, 'def');
SELECT groupArray(id) from tab where searchAny(message, 'abc def');
SELECT groupArray(id) from tab where searchAny(message, 'abcdef');

SELECT groupArray(id) from tab where searchAll(message, 'abc');
SELECT groupArray(id) from tab where searchAll(message, 'def');
SELECT groupArray(id) from tab where searchAll(message, 'abc def');
SELECT groupArray(id) from tab where searchAll(message, 'abcdef');

DROP TABLE tab;
