-- https://github.com/ClickHouse/ClickHouse/issues/59600
SELECT arrayReduce(toNullable('stddevSampOrNull'), [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayReduce(toNullable('median'), [toDecimal32OrNull(toFixedString('1', 1), 2), 8]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toFixedString('--- Int Empty ---', toLowCardinality(17)), arrayReduce(toNullable('avgOrNull'), [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayReduce('any', toNullable(3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayReduce(toLowCardinality('median'), [toLowCardinality(toNullable(8))]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- { echoOn }
SELECT arrayReduce('sum', []::Array(UInt8)) as a, toTypeName(a);
SELECT arrayReduce('sumOrNull', []::Array(UInt8)) as a, toTypeName(a);
SELECT arrayReduce('sum', [NULL]::Array(Nullable(UInt8))) as a, toTypeName(a);
SELECT arrayReduce('sum', [NULL, 10]::Array(Nullable(UInt8))) as a, toTypeName(a);
SELECT arrayReduce('any_respect_nulls', [NULL, 10]::Array(Nullable(UInt8))) as a, toTypeName(a);
SELECT arrayReduce('any_respect_nulls', [10, NULL]::Array(Nullable(UInt8))) as a, toTypeName(a);

SELECT arrayReduce('median', [toLowCardinality(toNullable(8))]) as t, toTypeName(t);
-- { echoOff }
