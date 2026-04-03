-- { echoOn }
-- Test tokensForLikePattern function with different tokenizers
-- Both tokens() and tokensForLikePattern() receive the SAME input string,
-- so the reader can directly see how the same string is tokenized differently.

-- splitByNonAlpha tokenizer (default)
SELECT 'splitByNonAlpha:';
SELECT tokens('hello_world'), tokensForLikePattern('hello_world');
SELECT tokens('hello\_world'), tokensForLikePattern('hello\_world');
SELECT tokens('hello\%world'), tokensForLikePattern('hello\%world');
SELECT tokens('%hello%'), tokensForLikePattern('%hello%');
SELECT tokens('%hello_world%'), tokensForLikePattern('%hello_world%');
SELECT tokens('%%__test'), tokensForLikePattern('%%__test');
SELECT tokens(''), tokensForLikePattern('');
SELECT tokens('%%%'), tokensForLikePattern('%%%');

-- ngrams tokenizer
SELECT 'ngrams:';
SELECT tokens('abcd', 'ngrams', 2), tokensForLikePattern('abcd', 'ngrams', 2);
SELECT tokens('%ab%', 'ngrams', 2), tokensForLikePattern('%ab%', 'ngrams', 2);
SELECT tokens('%abc%', 'ngrams', 3), tokensForLikePattern('%abc%', 'ngrams', 3);
SELECT tokens('abcd%', 'ngrams', 2), tokensForLikePattern('abcd%', 'ngrams', 2);
SELECT tokens('%ab_cd%', 'ngrams', 2), tokensForLikePattern('%ab_cd%', 'ngrams', 2);
SELECT tokens('%ab\_cd%', 'ngrams', 2), tokensForLikePattern('%ab\_cd%', 'ngrams', 2);

-- Unsupported tokenizers should throw error
SELECT 'unsupported tokenizers:';
SELECT tokensForLikePattern('%hello world%', 'splitByString', [' ']); -- { serverError BAD_ARGUMENTS }
SELECT tokensForLikePattern('%hello%', 'array'); -- { serverError BAD_ARGUMENTS }

-- Edge cases
SELECT 'edge cases:';
SELECT tokens('\\\\%test'), tokensForLikePattern('\\\\%test');
SELECT tokens('abc%d'), tokensForLikePattern('abc%d');
SELECT tokens('%%%%'), tokensForLikePattern('%%%%');
SELECT tokens('____'), tokensForLikePattern('____');

-- unicodeWord tokenizer
SELECT 'unicodeWord:';
SELECT tokens('hello', 'unicodeWord'), tokensForLikePattern('hello', 'unicodeWord');
SELECT tokens('你好世界', 'unicodeWord'), tokensForLikePattern('你好世界', 'unicodeWord');
SELECT tokens('hello', 'unicodeWord'), tokensForLikePattern('%hello%', 'unicodeWord');
SELECT tokens('hello_world', 'unicodeWord'), tokensForLikePattern('hello\_world%', 'unicodeWord');
SELECT tokens('你好世界', 'unicodeWord'), tokensForLikePattern('%你好%世界%', 'unicodeWord');
SELECT tokens('a:bc.d', 'unicodeWord'), tokensForLikePattern('a:b%c.d', 'unicodeWord');
SELECT tokens('测试数据', 'unicodeWord'), tokensForLikePattern('%测试，数据%', 'unicodeWord');
SELECT tokens('test_data', 'unicodeWord'), tokensForLikePattern('test\_%data', 'unicodeWord');
