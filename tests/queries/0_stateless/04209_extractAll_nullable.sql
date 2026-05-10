-- Functions returning Array (or other types that can't be Nullable) must accept
-- Nullable inputs and return their non-Nullable result type unchanged.
-- See https://github.com/ClickHouse/ClickHouse/issues/56977

-- extractAll on a Nullable column with a non-null value
SELECT extractAll(toNullable('Hello, world'), '(\\w+)');

-- extractAll on a Nullable column with NULL falls back to the default of Array(String) (empty array)
SELECT extractAll(materialize(CAST(NULL AS Nullable(String))), '(\\w+)');

-- extractAll on Nullable with mixed values
DROP TABLE IF EXISTS t_extract_null;
CREATE TABLE t_extract_null (s Nullable(String)) ENGINE = Memory;
INSERT INTO t_extract_null VALUES ('a b c'), (NULL), ('x y z');
SELECT s, extractAll(s, '(\\w+)') FROM t_extract_null ORDER BY s;
DROP TABLE t_extract_null;

-- Other "extract multiple strings to array" functions
SELECT extractAllGroups(toNullable('a=1, b=2'), '(\\w+)=(\\d+)');
SELECT extractAllGroupsHorizontal(toNullable('a=1, b=2'), '(\\w+)=(\\d+)');
SELECT extractAllGroupsVertical(toNullable('a=1, b=2'), '(\\w+)=(\\d+)');
SELECT extractGroups(toNullable('a=1'), '(\\w+)=(\\d+)');

-- splitBy* family
SELECT splitByChar(',', toNullable('a,b,c'));
SELECT splitByString('::', toNullable('a::b::c'));
SELECT splitByRegexp('\\W+', toNullable('hello, world!'));
SELECT splitByWhitespace(toNullable('hello world'));
SELECT splitByNonAlpha(toNullable('one,two;three'));
SELECT alphaTokens(toNullable('one,two;three'));

-- With NULL input, the function is evaluated over the default value of the nested column
-- (an empty string), so the result is f('') -- not the default of the result type.
-- For example, splitByChar(',', '') = [''] (one empty token), while extractAllGroups('', ...) = [].
SELECT extractAllGroups(materialize(CAST(NULL AS Nullable(String))), '(\\w+)=(\\d+)');
SELECT splitByChar(',', materialize(CAST(NULL AS Nullable(String))));
SELECT splitByString('::', materialize(CAST(NULL AS Nullable(String))));
SELECT splitByRegexp('\\W+', materialize(CAST(NULL AS Nullable(String))));
SELECT splitByWhitespace(materialize(CAST(NULL AS Nullable(String))));
SELECT splitByNonAlpha(materialize(CAST(NULL AS Nullable(String))));

-- The result type must remain non-Nullable (it would be illegal to wrap Array in Nullable)
SELECT toTypeName(extractAll(toNullable('x'), '(\\w+)'));
SELECT toTypeName(splitByChar(',', toNullable('a,b')));
SELECT toTypeName(extractAllGroups(toNullable('x'), '(\\w+)'));

-- LowCardinality(Nullable(String)) input must also work
SELECT extractAll(toLowCardinality(toNullable('Hello world')), '(\\w+)');
SELECT toTypeName(extractAll(toLowCardinality(toNullable('Hello')), '(\\w+)'));

-- With short_circuit_function_evaluation_for_nulls enabled, NULL rows must still be
-- materialised as f(default(input)) for non-Nullable result types -- not as
-- default(result_type) (which would substitute [] for `splitByChar` instead of ['']).
SET short_circuit_function_evaluation_for_nulls = 1;
SET short_circuit_function_evaluation_for_nulls_threshold = 0.5;
SELECT splitByChar(',', x) FROM (SELECT arrayJoin([NULL, NULL, NULL, '']::Array(Nullable(String))) AS x);
SELECT extractAll(x, '(\\w+)') FROM (SELECT arrayJoin([NULL, NULL, NULL, 'a b']::Array(Nullable(String))) AS x);

-- A `nullIf`-stripped row is semantically NULL but its underlying nested data still holds the
-- original value. The framework must normalise null rows to the default of the input type
-- before evaluating, otherwise it would compute `extractAll('hello world', ...)` for that row
-- and incorrectly return `['hello','world']` instead of `[]`.
SET short_circuit_function_evaluation_for_nulls = 0;
SELECT extractAll(nullIf(materialize('hello world'), materialize('hello world')), '(\\w+)');
SELECT
    number,
    extractAll(if(number = 0, nullIf(materialize('hello world'), materialize('hello world')), CAST('foo bar', 'Nullable(String)')), '(\\w+)')
FROM numbers(2);
