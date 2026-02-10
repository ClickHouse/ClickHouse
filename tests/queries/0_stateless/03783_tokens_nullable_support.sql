-- Test nullable support for tokens() function

SELECT '-- Test basic nullable string';
SELECT tokens(toNullable('hello world'));
SELECT tokens(materialize(toNullable('hello world')));
SELECT tokens(CAST(NULL AS Nullable(String)));
SELECT tokens('hello world', NULL);
SELECT tokens(toNullable(NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Test with different tokenizer';
SELECT tokens(toNullable('abc def'), 'ngrams', 3);
SELECT tokens(CAST(NULL AS Nullable(String)), 'ngrams', 3);
SELECT tokens(toNullable(NULL), 'ngrams', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Test with FixedString nullable variant';
SELECT tokens(toNullable(toFixedString('test', 10)));
SELECT tokens(toNullable(NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Test alphaTokens with nullable';
SELECT alphaTokens(toNullable('abc123def456'));
SELECT alphaTokens(CAST(NULL AS Nullable(String)));
SELECT alphaTokens(toNullable(NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Test arrayStringConcat with nullable array elements';
SELECT arrayStringConcat([toNullable('hello'), toNullable('world')]);
SELECT arrayStringConcat([toNullable('hello'), NULL, toNullable('world')]);
SELECT arrayStringConcat([toNullable('hello'), NULL, toNullable('world')], ', ');

SELECT '-- Test extractAllGroups with nullable';
SELECT extractAllGroups(toNullable('abc=111, def=222'), '(\\w+)=(\\w+)');
SELECT extractAllGroups(CAST(NULL AS Nullable(String)), '(\\w+)=(\\w+)');
SELECT extractAllGroups(NULL, '(\\w+)=(\\w+)');
SELECT extractAllGroups(toNullable(NULL), '(\\w+)=(\\w+)');

SELECT '-- Test extractAllGroupsHorizontal with nullable';
SELECT extractAllGroupsHorizontal(toNullable('abc=111, def=222'), '(\\w+)=(\\w+)');
SELECT extractAllGroupsHorizontal(CAST(NULL AS Nullable(String)), '(\\w+)=(\\w+)');
SELECT extractAllGroupsHorizontal(NULL, '(\\w+)=(\\w+)');
SELECT extractAllGroupsHorizontal(toNullable(NULL), '(\\w+)=(\\w+)');

SELECT '-- Test extractAllGroupsVertical with nullable';
SELECT extractAllGroupsVertical(toNullable('abc=111, def=222'), '(\\w+)=(\\w+)');
SELECT extractAllGroupsVertical(CAST(NULL AS Nullable(String)), '(\\w+)=(\\w+)');
SELECT extractAllGroupsVertical(NULL, '(\\w+)=(\\w+)');
SELECT extractAllGroupsVertical(toNullable(NULL), '(\\w+)=(\\w+)');

SELECT '-- Test ngrams with nullable';
SELECT ngrams(toNullable('ClickHouse'), 3);
SELECT ngrams(CAST(NULL AS Nullable(String)), 3);
SELECT ngrams(toNullable(NULL), 3); -- { serverError BAD_ARGUMENTS }
SELECT ngrams('ClickHouse', toNullable(3)); -- { serverError BAD_ARGUMENTS }
SELECT ngrams('ClickHouse', toNullable(CAST(NULL AS Nullable(INT)))); -- { serverError BAD_ARGUMENTS }
SELECT ngrams('ClickHouse', toNullable(NULL)); -- { serverError BAD_ARGUMENTS }

SELECT '-- Test splitByChar with nullable';
SELECT splitByChar(',', toNullable('a,b,c'));
SELECT splitByChar(',', CAST(NULL AS Nullable(String)));
SELECT splitByChar(',', toNullable(NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Test splitByNonAlpha with nullable';
SELECT splitByNonAlpha(toNullable('hello, world! 123'));
SELECT splitByNonAlpha(CAST(NULL AS Nullable(String)));
SELECT splitByNonAlpha(toNullable(NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Test splitByRegexp with nullable';
SELECT splitByRegexp('[0-9]+', toNullable('a1b2c3'));
SELECT splitByRegexp('[0-9]+', CAST(NULL AS Nullable(String)));
SELECT splitByRegexp('[0-9]+', toNullable(NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Test splitByString with nullable';
SELECT splitByString('||', toNullable('a||b||c'));
SELECT splitByString('||', CAST(NULL AS Nullable(String)));
SELECT splitByString('||', toNullable(NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Test splitByWhitespace with nullable';
SELECT splitByWhitespace(toNullable('hello   world  test'));
SELECT splitByWhitespace(CAST(NULL AS Nullable(String)));
SELECT splitByWhitespace(toNullable(NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
