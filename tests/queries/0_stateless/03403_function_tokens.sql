SELECT 'Constants: tokens should be constant';
SELECT 'Negative tests';
-- Must accept one to three arguments
SELECT tokens(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tokens('a', 'b', 'c', 'd'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- 1st arg must be String or FixedString
SELECT tokens(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- 2nd arg (if given) must be const String
SELECT tokens('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', toFixedString('b', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', materialize('b')); -- { serverError ILLEGAL_COLUMN }
-- 2nd arg (if given) must be a supported tokenizer
SELECT tokens('a', 'unsupported_tokenizer'); -- { serverError BAD_ARGUMENTS }
-- 3rd arg (if given) must be
--    const UInt8 (for "ngram")
SELECT tokens('a', 'ngrams', 'c'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'ngrams', toInt8(-1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'ngrams', toFixedString('c', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'ngrams', materialize(1)); -- { serverError ILLEGAL_COLUMN }
-- If 2nd arg is "ngram", then the 3rd arg must be between 1 and 8
SELECT tokens('a', 'ngrams', 0); -- { serverError BAD_ARGUMENTS}
SELECT tokens('a', 'ngrams', 9); -- { serverError BAD_ARGUMENTS}
--    const Array (for "split")
SELECT tokens('a', 'splitByString', 'c'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'splitByString', toInt8(-1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'splitByString', toFixedString('c', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'splitByString', materialize(['c'])); -- { serverError ILLEGAL_COLUMN }
SELECT tokens('a', 'splitByString', [1, 2]); -- { serverError INCORRECT_QUERY }
SELECT tokens('  a  bc d', 'splitByString', []); -- { serverError INCORRECT_QUERY }


SELECT 'Default tokenizer';

SELECT tokens('') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('abc+ def- foo! bar? baz= code; hello: world/ xäöüx') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('abc+ def- foo! bar? baz= code; hello: world/ xäöüx', 'splitByNonAlpha') AS tokenized, toTypeName(tokenized), isConstant(tokenized);

SELECT 'Ngram tokenizer';

SELECT tokens('', 'ngrams') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('abc def', 'ngrams') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('abc def', 'ngrams', 3) AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('abc def', 'ngrams', 8) AS tokenized, toTypeName(tokenized), isConstant(tokenized);

SELECT 'Split tokenizer';

SELECT tokens('', 'splitByString') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('  a  bc d', 'splitByString', [' ']) AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('()()a()bc()d', 'splitByString', ['()']) AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens(',()a(),bc,(),d,', 'splitByString', ['()', ',']) AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('\\a\n\\bc\\d\n', 'splitByString', ['\n', '\\']) AS tokenized, toTypeName(tokenized), isConstant(tokenized);

SELECT 'No-op tokenizer';

SELECT tokens('', 'array') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('abc def', 'array') AS tokenized, toTypeName(tokenized), isConstant(tokenized);

SELECT 'Special cases (not systematically tested)';
SELECT '-- FixedString inputs';
SELECT tokens(toFixedString('abc+ def- foo! bar? baz= code; hello: world/', 44)) AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT '-- non-const inputs';
SELECT tokens(materialize('abc+ def- foo! bar? baz= code; hello: world/')) AS tokenized, toTypeName(tokenized), isConstant(tokenized);

SELECT 'Column values: tokens should be non-constant';
SELECT 'Default tokenizer';

CREATE TABLE tab (
    id Int64,
    str String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO tab (id, str) VALUES (1, 'abc+ def-'), (2, 'hello: world/'), (3, 'xäöüx code;');

SELECT tokens(str, 'splitByNonAlpha') AS tokenized, toTypeName(tokenized), isConstant(tokenized) FROM tab;

DROP TABLE tab;

SELECT 'Ngram tokenizer';

CREATE TABLE tab (
    id Int64,
    str String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO tab (id, str) VALUES (1, 'abc def'), (2, 'ClickHouse');

SELECT tokens(str, 'ngrams', 3) AS tokenized, toTypeName(tokenized), isConstant(tokenized) FROM tab;

DROP TABLE tab;

SELECT 'Split tokenizer';

CREATE TABLE tab (
    id Int64,
    str String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO tab (id, str) VALUES (1, '()()a()bc()d'), (2, ',()a(),bc,(),d,');

SELECT tokens(str, 'splitByString', ['()', ',']) AS tokenized, toTypeName(tokenized), isConstant(tokenized) FROM tab;

DROP TABLE tab;

SELECT 'No-op tokenizer';

CREATE TABLE tab (
    id Int64,
    str String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO tab (id, str) VALUES (1, ''), (2, 'abc def');

SELECT tokens(str, 'array') AS tokenized, toTypeName(tokenized), isConstant(tokenized) FROM tab;

DROP TABLE tab;
SELECT tokens(materialize('abc+ def- foo! bar? baz= code; hello: world/'));

SELECT 'Sparse tokenizer';

SELECT tokens('', 'sparseGrams') AS tokenized;
SELECT tokens('abc def cba', 'sparseGrams') AS tokenized;
SELECT tokens('abc def cba', 'sparseGrams', 4, 10) AS tokenized;
SELECT tokens('abc def cba', 'sparseGrams', 4, 10, 6) AS tokenized;
