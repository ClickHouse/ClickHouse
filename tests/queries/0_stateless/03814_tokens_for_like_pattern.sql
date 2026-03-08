-- { echoOn }
-- Test tokensForLikePattern function with different tokenizers
-- Each test shows tokens() with plain string vs tokensForLikePattern() with equivalent LIKE pattern
-- This demonstrates how LIKE wildcards affect tokenization

-- splitByNonAlpha tokenizer (default)
SELECT 'splitByNonAlpha:';
SELECT tokens('hello_world'), tokensForLikePattern('hello_world');
SELECT tokens('hello_world'), tokensForLikePattern('hello\_world');
SELECT tokens('hello%world'), tokensForLikePattern('hello\%world');
SELECT tokens('hello'), tokensForLikePattern('%hello%');
SELECT tokens('hello_world'), tokensForLikePattern('%hello_world%');
SELECT tokens('test'), tokensForLikePattern('%%__test');
SELECT tokens(''), tokensForLikePattern('');
SELECT tokens(''), tokensForLikePattern('%%%');

-- ngrams tokenizer
SELECT 'ngrams:';
SELECT tokens('abcd', 'ngrams', 2), tokensForLikePattern('abcd', 'ngrams', 2);
SELECT tokens('ab', 'ngrams', 2), tokensForLikePattern('%ab%', 'ngrams', 2);
SELECT tokens('abc', 'ngrams', 3), tokensForLikePattern('%abc%', 'ngrams', 3);
SELECT tokens('abcd', 'ngrams', 2), tokensForLikePattern('abcd%', 'ngrams', 2);
SELECT tokens('ab_cd', 'ngrams', 2), tokensForLikePattern('%ab_cd%', 'ngrams', 2);
SELECT tokens('ab_cd', 'ngrams', 2), tokensForLikePattern('%ab\_cd%', 'ngrams', 2);

-- Unsupported tokenizers should throw error
SELECT 'unsupported tokenizers:';
SELECT tokensForLikePattern('%hello world%', 'splitByString', [' ']); -- { serverError BAD_ARGUMENTS }
SELECT tokensForLikePattern('%hello%', 'array'); -- { serverError BAD_ARGUMENTS }

-- Edge cases
SELECT 'edge cases:';
SELECT tokens('\\test'), tokensForLikePattern('\\\\%test');
SELECT tokens('abcd'), tokensForLikePattern('abc%d');
SELECT tokens(''), tokensForLikePattern('%%%%');
SELECT tokens(''), tokensForLikePattern('____');
