-- Tags: no-fasttest
-- no-fasttest: `generateULID` needs the ulid library, which the fast test build does not include.

-- `fuzzBits` and `generateULID` used to opt into the default implementation for constants, which
-- executes a function once and stamps the single result onto every row. That per-row behaviour is
-- covered by `05141_nondeterministic_functions_with_constant_arguments` and
-- `05151_generate_ulid_constant_argument`; this test pins down what removing the shortcut must not
-- break - the result shape, the ignored argument, and the reported errors.
-- https://github.com/ClickHouse/ClickHouse/issues/117224

SELECT 'shapes are preserved';
SELECT length(fuzzBits('aaaaaaaaaaaaaaaa', 0.4)), toTypeName(fuzzBits('aaaaaaaaaaaaaaaa', 0.4));
SELECT length(fuzzBits(toFixedString('aaaaaaaaaaaaaaaa', 16), 0.4)), toTypeName(fuzzBits(toFixedString('aaaaaaaaaaaaaaaa', 16), 0.4));
SELECT count(), uniqExact(length(x)) FROM (SELECT fuzzBits('abc', 0.0) AS x FROM numbers(10));
SELECT DISTINCT x FROM (SELECT fuzzBits('abc', 0.0) AS x FROM numbers(10));
SELECT count() FROM (SELECT fuzzBits(materialize('aaaaaaaaaaaaaaaa'), 0.4) FROM numbers(10));

SELECT 'the ignored argument of generateULID stays out of null propagation';
SELECT toTypeName(generateULID(NULL)), length(generateULID(NULL));
SELECT toTypeName(generateULID(toNullable('x'))), length(generateULID(toNullable('x')));
SELECT uniqExact(generateULID(toNullable('x'))) FROM numbers(100);

SELECT 'the rest of the family generates a value per row as well';
SELECT uniqExact(rand(1)) >= 99 FROM numbers(100); -- a UInt32 collision inside 100 draws is possible, if unlikely
SELECT uniqExact(generateUUIDv4('x')) FROM numbers(100);
SELECT uniqExact(generateSnowflakeID('x')) > 1 FROM numbers(100);

SELECT 'errors are still reported';
SELECT fuzzBits('abc', 2.0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT fuzzBits('abc', -1.0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT fuzzBits('abc', materialize(0.5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fuzzBits(1, 0.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generateULID(1, 2); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }
