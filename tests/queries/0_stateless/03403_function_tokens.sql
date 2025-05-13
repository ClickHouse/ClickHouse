-- Tags: no-fasttest
-- no-fasttest: Chinese tokenization relies on a 3rd party library

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
--    const String (for "chinese")
SELECT tokens('a', 'chinese', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'chinese', toFixedString('c', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'chinese', materialize('c')); -- { serverError ILLEGAL_COLUMN }
-- If 2nd arg is "ngram", then the 3rd arg must be between 2 and 8
SELECT tokens('a', 'ngram', 1); -- { serverError BAD_ARGUMENTS}
SELECT tokens('a', 'ngram', 9); -- { serverError BAD_ARGUMENTS}
-- If 2nd arg is "chinese", then the 3rd arg must be 'fine-granular' or 'coarse-granular'
SELECT tokens('a', 'chinese', 'shanghai'); -- { serverError BAD_ARGUMENTS}

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

SELECT 'Chinese tokenizer';
SELECT '-- coarse-grained (default)';
SELECT tokens('', 'chinese');
SELECT tokens('他来到了网易杭研大厦', 'chinese');
SELECT tokens('我来自北京邮电大学。', 'chinese');
SELECT tokens('南京市长江大桥', 'chinese');
SELECT tokens('我来自北京邮电大学。。。学号123456', 'chinese');
SELECT tokens('小明硕士毕业于中国科学院计算所，后在日本京都大学深造', 'chinese');
SELECT '-- fine-grained';
SELECT tokens('', 'chinese', 'fine-grained');
SELECT tokens('他来到了网易杭研大厦', 'chinese', 'fine-grained');
SELECT tokens('我来自北京邮电大学。', 'chinese', 'fine-grained');
SELECT tokens('南京市长江大桥', 'chinese', 'fine-grained');
SELECT tokens('我来自北京邮电大学。。。学号123456', 'chinese', 'fine-grained');
SELECT tokens('小明硕士毕业于中国科学院计算所，后在日本京都大学深造', 'chinese', 'fine-grained');

SELECT 'Special cases (not systematically tested)';
SELECT '-- FixedString inputs';
SELECT tokens(toFixedString('abc+ def- foo! bar? baz= code; hello: world/', 44));
SELECT '-- non-const inputs';
SELECT tokens(materialize('abc+ def- foo! bar? baz= code; hello: world/'));
