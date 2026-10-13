-- Tags: no-fasttest
-- no-fasttest: `ULIDStringToDateTime` and `lemmatize` are not available in the fast test build.

-- Timezone arguments accept a constant `FixedString` as well as a constant `String`.
SELECT ULIDStringToDateTime('01GNB2S2FGN2P93QPXDNB4EN2R', toFixedString('UTC', 3));
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), toFixedString('UTC', 3));
SELECT toDateTime64OrNull('2020-01-01 00:00:00', 3, toFixedString('UTC', 3));
SELECT toTypeName(parseDateTime64BestEffortOrZero('2020-01-01', 3, toFixedString('UTC', 3)));
SELECT toTypeName(toDateTimeOrZero('2020-01-01 00:00:00', toFixedString('UTC', 3)));
SELECT makeDateTime(2023, 1, 1, 0, 0, 0, toFixedString('UTC', 3));
SELECT makeDateTime64(2023, 1, 1, 0, 0, 0, 0, 3, toFixedString('UTC', 3));
SELECT YYYYMMDDhhmmssToDateTime(20230101000000, toFixedString('UTC', 3));
SELECT YYYYMMDDhhmmssToDateTime64(20230101000000, 3, toFixedString('UTC', 3));

-- Constant-only arguments are rejected during analysis.
SELECT substringIndex('a.b.c', materialize('.'), 1); -- { serverError ILLEGAL_COLUMN }
SELECT substringIndex('a.b.c', '.', 1);
SELECT roundDown(5, materialize([1, 4, 8])); -- { serverError ILLEGAL_COLUMN }
SELECT roundDown(5, [1, 4, 8]);
SELECT minSampleSizeContinuous(20, 10, materialize(0.05), 0.8, 0.05); -- { serverError ILLEGAL_COLUMN }
SELECT minSampleSizeConversion(0.9, materialize(0.01), 0.8, 0.05); -- { serverError ILLEGAL_COLUMN }
SELECT __patchPartitionID('all', materialize('x')); -- { serverError ILLEGAL_COLUMN }
SET allow_experimental_nlp_functions = 1;
SELECT lemmatize(materialize('en'), 'wolves'); -- { serverError ILLEGAL_COLUMN }
