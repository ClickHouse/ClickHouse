-- Tests error paths for functions whose validation was refactored in PR #97672
-- (manual validation replaced with validateFunctionArguments framework).
-- Only functions that had NO existing error-path coverage are included.

-- appendTrailingCharIfAbsent
SELECT appendTrailingCharIfAbsent('hello', '/');
SELECT appendTrailingCharIfAbsent('hello/', '/');
SELECT appendTrailingCharIfAbsent(123, '/'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT appendTrailingCharIfAbsent('hello', 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- monthName
SELECT monthName(toDate('2023-01-15'));
SELECT monthName(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- conv
SELECT conv('ff', 16, 10);
SELECT conv(255, 10, 16);
SELECT conv('ff', 'abc', 10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- lowCardinalityIndices / lowCardinalityKeys
SELECT lowCardinalityIndices(toLowCardinality('hello'));
SELECT lowCardinalityKeys(toLowCardinality('hello'));
SELECT lowCardinalityIndices('hello'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT lowCardinalityKeys('hello'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- randomPrintableASCII
SELECT length(randomPrintableASCII(10));
SELECT randomPrintableASCII('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- randomString
SELECT length(randomString(10));
SELECT randomString('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- ngrams
SELECT ngrams('ClickHouse', 3);
SELECT ngrams(123, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- dynamicType
SELECT dynamicType(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
