-- Tags: no-fasttest
-- ULID is not available in the fast test build.
-- https://github.com/ClickHouse/ClickHouse/issues/117224
-- `fuzzBits` and `generateULID` are not deterministic, but they used to opt into the default
-- implementation for constants, which executes a function once and stamps the single result onto
-- every row. Each row must receive an independently generated value, as the rest of the family does.

SELECT uniqExact(fuzzBits('aaaaaaaaaaaaaaaa', 0.4)) FROM numbers(100);
SELECT uniqExact(fuzzBits(materialize('aaaaaaaaaaaaaaaa'), 0.4)) FROM numbers(100);
SELECT uniqExact(fuzzBits(toFixedString('aaaaaaaaaaaaaaaa', 16), 0.4)) FROM numbers(100);
SELECT uniqExact(generateULID('x')) FROM numbers(100);
SELECT uniqExact(generateULID()) FROM numbers(100);

SELECT 'shapes are preserved';
SELECT length(fuzzBits('aaaaaaaaaaaaaaaa', 0.4)), toTypeName(fuzzBits('aaaaaaaaaaaaaaaa', 0.4));
SELECT length(fuzzBits(toFixedString('aaaaaaaaaaaaaaaa', 16), 0.4)), toTypeName(fuzzBits(toFixedString('aaaaaaaaaaaaaaaa', 16), 0.4));
SELECT count(), uniqExact(length(x)) FROM (SELECT fuzzBits('abc', 0.0) AS x FROM numbers(10));
SELECT DISTINCT x FROM (SELECT fuzzBits('abc', 0.0) AS x FROM numbers(10));
SELECT count() FROM (SELECT fuzzBits('aaaaaaaaaaaaaaaa', 0.4) FROM numbers(0));

SELECT 'the rest of the family';
SELECT uniqExact(rand(1)) FROM numbers(100);
SELECT uniqExact(generateUUIDv4('x')) FROM numbers(100);
SELECT uniqExact(generateSnowflakeID('x')) > 1 FROM numbers(100);

SELECT 'the argument bypasses common subexpression elimination';
SELECT generateULID(1) != generateULID(2);

SELECT 'errors are still reported';
SELECT fuzzBits('abc', 2.0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT fuzzBits('abc', -1.0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT fuzzBits('abc', materialize(0.5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fuzzBits(1, 0.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generateULID(1, 2); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }
