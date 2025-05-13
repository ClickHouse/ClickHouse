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
SELECT tokens('a', 'ngram', 'c'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'ngram', toInt8(-1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'ngram', toFixedString('c', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'ngram', materialize(1)); -- { serverError ILLEGAL_COLUMN }
-- If 2nd arg is "ngram", then the 3rd arg must be between 2 and 8
SELECT tokens('a', 'ngram', 1); -- { serverError BAD_ARGUMENTS}
SELECT tokens('a', 'ngram', 9); -- { serverError BAD_ARGUMENTS}

SELECT 'Default tokenizer';

SELECT tokens('');
SELECT tokens('abc+ def- foo! bar? baz= code; hello: world/');
SELECT tokens('abc+ def- foo! bar? baz= code; hello: world/', 'default');

SELECT 'Ngram tokenizer';

SELECT tokens('', 'ngram') AS tokenized;
SELECT tokens('abc def', 'ngram') AS tokenized;
SELECT tokens('abc def', 'ngram', 3) AS tokenized;
SELECT tokens('abc def', 'ngram', 8) AS tokenized;

SELECT 'NoOp tokenizer';

SELECT tokens('', 'noop') AS tokenized;
SELECT tokens('abc def', 'noop') AS tokenized;

SELECT 'Special cases (not systematically tested)';
SELECT '-- FixedString inputs';
SELECT tokens(toFixedString('abc+ def- foo! bar? baz= code; hello: world/', 44));
SELECT '-- non-const inputs';
SELECT tokens(materialize('abc+ def- foo! bar? baz= code; hello: world/'));
