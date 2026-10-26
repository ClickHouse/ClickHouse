-- Test: assertInfinity and assertNaN throw CANNOT_PARSE_INPUT_ASSERTION_FAILED
-- when the float text parser (readFloatTextFastImpl, throw_exception=true path)
-- encounters a token that starts with the infinity/NaN prefix but is not valid.
--
-- src/IO/readFloatText.cpp
--   assertInfinity() line 59-60: called when input starts with 'i'/'I' and
--     parseInfinity() fails (e.g., "Ingg" has 'g' where 'f' is expected).
--     Previously untested because CI float inputs are always valid.
--   assertNaN() line 65-66: called when input starts with 'n'/'N' and
--     parseNaN() fails (e.g., "NaXX" has 'X' where the third NaN char is expected).
--     Same reason.
--   readFloatTextFastImpl bool variant: assertOrParseNaN<false> and
--     assertOrParseInfinity<false> return false when the prefix is malformed;
--     the false branches and the `return false` after them were never exercised in CI.
--     The conversion functions reach this parser only with `precise_float_parsing = 0`:
--     with the default `1`, `tryReadFloatTextPrecise` returns false from its
--     `parseFloatFromRange` fast path before it looks at the NaN/infinity prefix.

-- Throw variant: assertInfinity failure.
-- "Ingg": 'I','n','g' - 'g' does not match 'f'/'F', so parseInfinity returns false.
SELECT * FROM format('TSV', 'x Float32', 'Ingg'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Throw variant: assertInfinity failure, lowercase prefix.
SELECT * FROM format('TSV', 'x Float64', 'inXX'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Throw variant: assertNaN failure.
-- "NaXX": 'N','a','X' - 'X' does not match 'n'/'N', so parseNaN returns false.
SELECT * FROM format('TSV', 'x Float32', 'NaXX'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Throw variant: assertNaN failure, lowercase prefix.
SELECT * FROM format('TSV', 'x Float64', 'naZZ'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Try variants: the fast parser, which the conversions use only with precise_float_parsing = 0.
SET precise_float_parsing = 0;

-- assertOrParseNaN<false> returns false: "NaXX" takes the NaN case of
-- readFloatTextFastImpl<Float32, bool>, parseNaN fails at the third char -> NULL.
SELECT toFloat32OrNull('NaXX') AS null_on_bad_nan;

-- assertOrParseInfinity<false> returns false.
SELECT toFloat32OrNull('Ingg') AS null_on_bad_inf;

-- Confirm valid infinity and NaN still parse correctly (regression guard).
SELECT toFloat32OrNull('inf') AS valid_inf, toFloat32OrNull('nan') AS valid_nan;
