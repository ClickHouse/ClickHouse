-- Exponents whose magnitude exceeds the Int32 (and Int64) range must saturate, not silently become 0.
-- Previously the parser ignored the overflow and treated such exponents as 1e0.

-- Tiny sample rate: denominator saturates to 2^128 - 1.
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e-3000000000');
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e-2147483648');
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e-2147483649');
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e-9999999999999999999999999999999999999999');

-- Huge sample rate: numerator saturates to 2^128 - 1.
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e3000000000');
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e2147483648');

-- Ordinary exponents keep working.
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1.23e-1');
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e2');

-- Digit separators are stripped, both in the mantissa and in the exponent, so the grouped
-- spelling of an overflowing exponent takes the same saturation path.
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1_000');
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e2_147_483_648');
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e-2_147_483_648');

-- Malformed exponent (no digits) must fail to parse.
SELECT formatQuerySingleLine('SELECT 1 FROM numbers(1) SAMPLE 1e'); -- { serverError SYNTAX_ERROR }
