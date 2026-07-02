-- Corrected declarative signatures for the conversion, tokenizer, hashing and
-- constant-option functions (function-signatures DSL). These signatures now spell out the
-- mandatory / optional positions and the constant positions that each function's validator or
-- executor actually enforces, so `system.functions.signature` no longer advertises argument
-- shapes that would be rejected at execution. Continuation of the argument-validation work in
-- `04521_function_signature_argument_validation`.
-- https://github.com/ClickHouse/ClickHouse/pull/104948

-- toDateTime64 / toTime64: the scale is mandatory. For DateTime64 the timezone is an optional
-- trailing position that requires the scale first (spelled out as explicit prefixes rather than
-- independent optional groups); Time64 has no timezone.
SELECT signature FROM system.functions WHERE name = 'toDateTime64';
SELECT signature FROM system.functions WHERE name = 'toTime64';

-- The 'OrZero' / 'OrNull' / 'parse*BestEffort' string converters take an optional scale and,
-- for DateTime64, an optional timezone that requires the scale first.
SELECT signature FROM system.functions WHERE name = 'toDateTime64OrZero';
SELECT signature FROM system.functions WHERE name = 'toDateTime64OrNull';
SELECT signature FROM system.functions WHERE name = 'parseDateTime64BestEffortOrNull';

-- isDecimalOverflow: the optional precision must be constant.
SELECT signature FROM system.functions WHERE name = 'isDecimalOverflow';

-- arrayShuffle / arrayPartialShuffle: the optional seed / limit positions are constant.
SELECT signature FROM system.functions WHERE name = 'arrayShuffle';
SELECT signature FROM system.functions WHERE name = 'arrayPartialShuffle';

-- ngramSimHash / ngramMinHash: the optional shingle-size / hash-count options are constant.
SELECT signature FROM system.functions WHERE name = 'ngramSimHash';
SELECT signature FROM system.functions WHERE name = 'ngramMinHash';

-- Token generators: the separator (splitByChar / splitByString / splitByRegexp) and the
-- max_substrings option are constant.
SELECT signature FROM system.functions WHERE name = 'splitByChar';
SELECT signature FROM system.functions WHERE name = 'splitByString';
SELECT signature FROM system.functions WHERE name = 'splitByRegexp';
SELECT signature FROM system.functions WHERE name = 'splitByWhitespace';
SELECT signature FROM system.functions WHERE name = 'alphaTokens';
SELECT signature FROM system.functions WHERE name = 'sparseGrams';

-- A row-varying value in a constant-only position is rejected during analysis instead of only
-- failing at execution.
SELECT isDecimalOverflow(materialize(toDecimal32(1, 2)), materialize(toUInt8(9))); -- { serverError ILLEGAL_COLUMN }
SELECT arrayShuffle([1, 2, 3], materialize(toUInt64(1234))); -- { serverError ILLEGAL_COLUMN }
SELECT arrayPartialShuffle([1, 2, 3], materialize(toUInt64(1))); -- { serverError ILLEGAL_COLUMN }
SELECT ngramSimHash('hello world', materialize(toUInt64(4))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ngramMinHash('hello world', materialize(toUInt64(4)), materialize(toUInt64(2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT splitByChar(materialize(','), 'a,b,c'); -- { serverError ILLEGAL_COLUMN }
SELECT splitByChar(',', 'a,b,c,d', materialize(toUInt64(2))); -- { serverError ILLEGAL_COLUMN }

-- Valid forms keep working.
SELECT toTypeName(toDateTime64(0, 3, 'UTC'));
SELECT toTypeName(toTime64(0, 3));
SELECT isDecimalOverflow(toDecimal32(1, 2), 9);
SELECT length(arrayShuffle([1, 2, 3], 1234));
SELECT splitByChar(',', 'a,b,c');
