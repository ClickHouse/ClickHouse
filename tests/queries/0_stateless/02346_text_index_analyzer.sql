-- Tests the __textIndexAnalyzer function.

SELECT 'Negative tests';

-- Must accept exactly two arguments
SELECT __textIndexAnalyzer(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', 'a', 'b'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- 1st arg must be const String
SELECT __textIndexAnalyzer(1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __textIndexAnalyzer(materialize('tokenizer = splitByNonAlpha'), 'a'); -- { serverError ILLEGAL_COLUMN }
-- 2nd arg must be String or FixedString
SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- tokenizer is required
SELECT __textIndexAnalyzer('dictionary_block_size = 512', 'a'); -- { serverError BAD_ARGUMENTS }
-- unsupported tokenizer
SELECT __textIndexAnalyzer('tokenizer = unsupported', 'a'); -- { serverError BAD_ARGUMENTS }
-- invalid syntax in definition
SELECT __textIndexAnalyzer('tokenizer = splitByString(,)', 'a'); -- { serverError SYNTAX_ERROR }
SELECT __textIndexAnalyzer('tokenizer = ngrams(-1)', 'a'); -- { serverError BAD_ARGUMENTS }

SELECT 'splitByNonAlpha tokenizer';

SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', '');
SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', 'abc+ def- foo! bar?');
SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', 'xäöüx code; hello: world/');

SELECT 'ngrams tokenizer';

SELECT __textIndexAnalyzer('tokenizer = ngrams(3)', '');
SELECT __textIndexAnalyzer('tokenizer = ngrams(3)', 'abc def');
SELECT __textIndexAnalyzer('tokenizer = ngrams(5)', 'abcdef');

SELECT 'splitByString tokenizer';

SELECT __textIndexAnalyzer('tokenizer = splitByString([\',\'])', 'a,bb,ccc');
SELECT __textIndexAnalyzer('tokenizer = splitByString([\'::\'])', 'a::bb::ccc');

SELECT 'asciiCJK tokenizer';

SELECT __textIndexAnalyzer('tokenizer = asciiCJK', 'hello world');
SELECT __textIndexAnalyzer('tokenizer = asciiCJK', '你好世界');
SELECT __textIndexAnalyzer('tokenizer = asciiCJK', 'hello_world');

SELECT 'array tokenizer';

SELECT __textIndexAnalyzer('tokenizer = array', '');
SELECT __textIndexAnalyzer('tokenizer = array', 'abc def');

SELECT 'With preprocessor';

SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha, preprocessor = lower({input})', 'Hello World ABC');
SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha, preprocessor = upper({input})', 'Hello World');

SELECT 'Ignores extra text index params';

SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha, dictionary_block_size = 8, posting_list_block_size = 1024', 'hello world');

SELECT 'FixedString input';
SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', toFixedString('abc def', 7));

SELECT 'Non-const input';

SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', materialize('abc def'));

SELECT 'NULL input';

SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', NULL);
SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', materialize(NULL));

SELECT 'Column values';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'abc+ DEF-'), (2, 'heLlO: World/'), (3, 'xäöüx code;');

SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha', s) FROM tab ORDER BY id;
SELECT __textIndexAnalyzer('tokenizer = splitByNonAlpha, preprocessor = lower({input})', s) FROM tab ORDER BY id;

DROP TABLE tab;
