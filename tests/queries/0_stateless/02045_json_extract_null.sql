-- Tags: no-fasttest

-- to avoid merging Tags and echoOn
SELECT 1 FORMAT Null;

-- { echoOn }
SELECT JSONExtractInt('[1]', toNullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractUInt('[1]', toNullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractBool('[1]', toNullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractFloat('[1]', toNullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractString('["a"]', toNullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT JSONExtractArrayRaw('["1"]', toNullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractKeysAndValuesRaw('["1"]', toNullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractKeysAndValues('["1"]', toNullable(1)); -- { serverError ILLEGAL_COLUMN }

SELECT JSONExtract('[1]', toNullable(1), 'Nullable(Int)');
SELECT JSONExtract('[1]', toNullable(1), 'Nullable(UInt8)');
SELECT JSONExtract('[1]', toNullable(1), 'Nullable(Bool)');
SELECT JSONExtract('[1]', toNullable(1), 'Nullable(Float)');
SELECT JSONExtract('["a"]', toNullable(1), 'Nullable(String)');
