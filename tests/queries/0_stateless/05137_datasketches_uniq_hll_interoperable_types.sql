-- Tags: no-fasttest
-- Tag no-fasttest: needs the DataSketches library.

-- `uniqHLL` promises an Apache DataSketches-compatible state, so it must only accept argument types
-- that DataSketches itself can encode. Everything else used to fall back to hashing ClickHouse's
-- internal representation (or the raw bytes), producing sketches no other DataSketches
-- implementation could reproduce.

SELECT 'accepted types';
SELECT uniqHLL(toInt8(number)) FROM numbers(10);
SELECT uniqHLL(toUInt32(number)) FROM numbers(10);
SELECT uniqHLL(toInt64(number)) FROM numbers(10);
SELECT uniqHLL(toFloat64(number)) FROM numbers(10);
SELECT uniqHLL(toDate(number)) FROM numbers(10);
SELECT uniqHLL(toDate32(number)) FROM numbers(10);
SELECT uniqHLL(toDateTime(number)) FROM numbers(10);
SELECT uniqHLL(toString(number)) FROM numbers(10);
SELECT uniqHLL(toFixedString(toString(number), 4)) FROM numbers(10);
SELECT uniqHLL(CAST(toString(number % 3), 'Enum8(\'0\' = 0, \'1\' = 1, \'2\' = 2)')) FROM numbers(10);

SELECT 'rejected types';
SELECT uniqHLL(toDecimal64(number, 2)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqHLL(toDecimal32(number, 2)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqHLL(toDateTime64(number, 3)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqHLL(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqHLL(toIPv6('::1')) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqHLL(toInt128(number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqHLL(toUInt256(number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqHLL(tuple(number, number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqHLL([number]) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqHLL(number, number) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT uniqHLL(10)(number) FROM numbers(10); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }
