-- Test nullable support for tokens() function

-- Test basic nullable string
SELECT tokens(CAST('hello world' AS Nullable(String)));

-- Test NULL value
SELECT tokens(CAST(NULL AS Nullable(String)));

-- Test with table containing nullable column
DROP TABLE IF EXISTS test_tokens_nullable;
CREATE TABLE test_tokens_nullable (id UInt32, text Nullable(String)) ENGINE = Memory;
INSERT INTO test_tokens_nullable VALUES (1, 'foo bar'), (2, NULL), (3, 'test data');

SELECT id, tokens(text) FROM test_tokens_nullable ORDER BY id;

DROP TABLE test_tokens_nullable;

-- Test with different tokenizers
SELECT tokens(CAST('abc def' AS Nullable(String)), 'ngrams', 3);

SELECT tokens(CAST(NULL AS Nullable(String)), 'ngrams', 3);

-- Test with FixedString nullable variant
SELECT tokens(CAST('test' AS Nullable(FixedString(10))));

SELECT tokens(CAST(NULL AS Nullable(FixedString(10))));

-- Test alphaTokens with nullable
SELECT alphaTokens(CAST('abc123def456' AS Nullable(String)));
SELECT alphaTokens(CAST(NULL AS Nullable(String)));

-- Test arrayStringConcat with nullable array elements
SELECT arrayStringConcat(CAST(['hello', 'world'] AS Array(Nullable(String))));
SELECT arrayStringConcat(CAST(['hello', NULL, 'world'] AS Array(Nullable(String))));
SELECT arrayStringConcat(CAST(['hello', NULL, 'world'] AS Array(Nullable(String))), ', ');

-- Test extractAllGroupsVertical with nullable
SELECT extractAllGroupsVertical(CAST('abc=111, def=222' AS Nullable(String)), '(\\w+)=(\\w+)');
SELECT extractAllGroupsVertical(CAST(NULL AS Nullable(String)), '(\\w+)=(\\w+)');

-- Test ngrams with nullable
SELECT ngrams(CAST('ClickHouse' AS Nullable(String)), 3);
SELECT ngrams(CAST(NULL AS Nullable(String)), 3);

-- Test splitByChar with nullable
SELECT splitByChar(',', CAST('a,b,c' AS Nullable(String)));
SELECT splitByChar(',', CAST(NULL AS Nullable(String)));

-- Test splitByNonAlpha with nullable
SELECT splitByNonAlpha(CAST('hello, world! 123' AS Nullable(String)));
SELECT splitByNonAlpha(CAST(NULL AS Nullable(String)));

-- Test splitByRegexp with nullable
SELECT splitByRegexp('[0-9]+', CAST('a1b2c3' AS Nullable(String)));
SELECT splitByRegexp('[0-9]+', CAST(NULL AS Nullable(String)));

-- Test splitByString with nullable
SELECT splitByString('||', CAST('a||b||c' AS Nullable(String)));
SELECT splitByString('||', CAST(NULL AS Nullable(String)));

-- Test splitByWhitespace with nullable
SELECT splitByWhitespace(CAST('hello   world  test' AS Nullable(String)));
SELECT splitByWhitespace(CAST(NULL AS Nullable(String)));