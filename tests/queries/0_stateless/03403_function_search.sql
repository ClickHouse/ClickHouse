SELECT 'Negative tests';
-- Must accept two to four arguments
SELECT searchAny(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT searchAny('a', 'b', 'c', 'd', 'e'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT searchAll(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT searchAll('a', 'b', 'c', 'd', 'e'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- 1st and 2nd arg must be String or FixedString
SELECT searchAny('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT searchAny(1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT searchAny(1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT searchAll('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT searchAll(1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT searchAll(1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- 2nd arg must be const String
SELECT searchAny('a', toFixedString('b', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT searchAny('a', materialize('b')); -- { serverError ILLEGAL_COLUMN }
SELECT searchAll('a', toFixedString('b', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT searchAll('a', materialize('b')); -- { serverError ILLEGAL_COLUMN }
-- 3rd arg (if given) must be const String
SELECT searchAny('a', 'b', toFixedString('b', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT searchAny('a', 'b', materialize('b')); -- { serverError ILLEGAL_COLUMN }
SELECT searchAll('a', 'b', toFixedString('b', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT searchAll('a', 'b', materialize('b')); -- { serverError ILLEGAL_COLUMN }
-- 3nd arg (if given) must be a supported tokenizer
SELECT searchAny('a', 'b', 'unsupported_tokenizer'); -- { serverError BAD_ARGUMENTS }
SELECT searchAll('a', 'b', 'unsupported_tokenizer'); -- { serverError BAD_ARGUMENTS }

SELECT 'Default tokenizer';

SELECT searchAny('', 'abc');
SELECT searchAny('abc+ def- foo!', 'foo! bar?');
SELECT searchAny('abc+ def- bar?', 'def- foo!', 'default');
SELECT searchAny('abc+ def- foo!', 'abc foo');
SELECT searchAny('abc+ def- bar?', 'abc bar', 'default');
SELECT searchAny('abc+ def- foo!', 'bar ab');
SELECT searchAny('abc+ def- bar?', 'foo de', 'default');

SELECT searchAll('', 'abc');
SELECT searchAll('abc+ def- foo!', 'foo! bar?');
SELECT searchAll('abc+ def- bar?', 'def- foo!', 'default');
SELECT searchAll('abc+ def- foo!', 'foo abc+');
SELECT searchAll('abc+ def- bar?', 'def- bar', 'default');

SELECT 'Ngram tokenizer';

SELECT searchAny('', 'abc', 'ngram');
SELECT searchAny('abc def', 'foo def', 'ngram');
SELECT searchAny('abc def', 'bar abc', 'ngram', 3);
SELECT searchAny('abc def', 'bar abc', 'ngram', 2);
SELECT searchAny('abc def', 'abc def', 'ngram', 8);

SELECT searchAll('', 'abc', 'ngram');
SELECT searchAll('abc def', 'foo def', 'ngram');
SELECT searchAll('abc def', 'abc def', 'ngram');
SELECT searchAll('abc def', 'bar abc', 'ngram', 3);
SELECT searchAll('abc def', 'def abc', 'ngram', 3);
SELECT searchAll('abc def', 'bar abc', 'ngram', 2);
SELECT searchAll('abc def', 'abc def', 'ngram', 8);

SELECT 'NoOp tokenizer';

SELECT searchAny('', 'abc', 'noop');
SELECT searchAny('abc def', 'def abc', 'noop');
SELECT searchAny('abc def', 'abc def', 'noop');
SELECT searchAny('abcdef', 'abcdef', 'noop');

SELECT searchAll('', 'abc', 'noop');
SELECT searchAll('abc def', 'def abc', 'noop');
SELECT searchAll('abc def', 'abc def', 'noop');
SELECT searchAll('abcdef', 'abcdef', 'noop');
