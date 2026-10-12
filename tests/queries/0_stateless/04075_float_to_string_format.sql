-- Verify float-to-string formatting matches the expected format:
--   Fixed notation for decimal exponent in [-6, 20], scientific notation outside.
--   Scientific notation: no '+' on positive exponent, no zero-padding.
--   NaN never has a sign. Inf preserves sign.

-- Float64: Fixed notation boundaries
SELECT '--- Float64 fixed/scientific boundaries ---';
SELECT 0.000001;             -- dec_exp = -6 → fixed
SELECT 1e-7;                 -- dec_exp = -7 → scientific
SELECT 0.00001;              -- dec_exp = -5 → fixed
SELECT 0.0001;               -- dec_exp = -4 → fixed
SELECT 0.1;                  -- dec_exp = -1 → fixed
SELECT 1000000000000000;     -- 1e15, dec_exp = 15 → fixed
SELECT 10000000000000000;    -- 1e16, dec_exp = 16 → fixed
SELECT 100000000000000000000; -- 1e20, dec_exp = 20 → fixed
SELECT 1e21;                 -- dec_exp = 21 → scientific

-- Float64: Scientific notation exponent format
SELECT '--- Float64 scientific exponent format ---';
SELECT 1e-7;                 -- single-digit negative: e-7
SELECT 1e-20;                -- two-digit negative: e-20
SELECT 1e-100;               -- three-digit negative: e-100
SELECT 1e21;                 -- two-digit positive: e21
SELECT 1e100;                -- three-digit positive: e100
SELECT 1.7976931348623157e308; -- max Float64

-- Float64: Values around thresholds with multiple significant digits
SELECT '--- Float64 multi-digit threshold values ---';
SELECT 1.23456789e-5;        -- dec_exp = -5, fixed
SELECT 1.23456789e-6;        -- dec_exp = -6, fixed
SELECT 1.23456789e-7;        -- dec_exp = -7, scientific
SELECT 9.99e19;              -- dec_exp = 19, fixed
SELECT 9.99e20;              -- dec_exp = 20, fixed
SELECT 9.99e21;              -- dec_exp = 21, scientific
SELECT 3.14159e10;           -- dec_exp = 10, fixed

-- Float64: Full precision values
SELECT '--- Float64 full precision ---';
SELECT 1.2345678901234568;   -- 17 significant digits, small
SELECT 9.999999999999998e20; -- 17 significant digits near boundary
SELECT 1.0000000000000002;   -- smallest increment above 1.0

-- Float64: Special values
SELECT '--- Float64 special values ---';
SELECT nan::Float64;
SELECT -nan::Float64;        -- must NOT show '-nan'
SELECT toString(-nan::Float64);
SELECT inf::Float64;
SELECT -inf::Float64;
SELECT 0::Float64;
SELECT -0.0::Float64;

-- Float64: Subnormal
SELECT '--- Float64 subnormal ---';
SELECT reinterpretAsFloat64(unhex('0100000000000000')); -- smallest subnormal (5e-324)
SELECT reinterpretAsFloat64(unhex('FFFFFFFFFFFF0F00')); -- largest subnormal
SELECT 2.2250738585072014e-308; -- smallest normal

-- Float32: Fixed notation boundaries
SELECT '--- Float32 fixed/scientific boundaries ---';
SELECT 0.000001::Float32;    -- dec_exp = -6 → fixed
SELECT 1e-7::Float32;        -- dec_exp = -7 → scientific
SELECT 0.00001::Float32;     -- dec_exp = -5 → fixed
SELECT 0.0001::Float32;      -- dec_exp = -4 → fixed
SELECT 10000000::Float32;    -- 1e7, dec_exp = 7 → fixed
SELECT 100000000::Float32;   -- 1e8, dec_exp = 8 → fixed
SELECT CAST(1e20, 'Float32'); -- dec_exp = 20 → fixed
SELECT CAST(1e21, 'Float32'); -- dec_exp = 21 → scientific

-- Float32: Scientific notation exponent format
SELECT '--- Float32 scientific exponent format ---';
SELECT 1e-20::Float32;       -- two-digit negative
SELECT 3.4e38::Float32;      -- near max Float32
SELECT 1e30::Float32;        -- large positive exponent

-- Float32: Special values
SELECT '--- Float32 special values ---';
SELECT nan::Float32;
SELECT -nan::Float32;
SELECT inf::Float32;
SELECT -inf::Float32;
SELECT 0::Float32;

-- Float32: Small values
SELECT '--- Float32 small values ---';
SELECT 1.1754942e-38::Float32; -- smallest normal approx
SELECT 3.14::Float32;

-- BFloat16
SELECT '--- BFloat16 ---';
SELECT 3.14::BFloat16;
SELECT CAST(1e10, 'BFloat16');
SELECT 1e-7::BFloat16;
SELECT nan::BFloat16;
SELECT -nan::BFloat16;
SELECT inf::BFloat16;
SELECT -inf::BFloat16;

-- Negative floating-point values
SELECT '--- Negative FP values ---';
SELECT -0.000001;              -- dec_exp = -6 → fixed
SELECT -1e-7;                  -- dec_exp = -7 → scientific
SELECT -1.23456789e-7;         -- scientific with sign
SELECT -9.99e20;               -- dec_exp = 20, fixed
SELECT -9.99e21;               -- dec_exp = 21, scientific
SELECT -3.14::Float32;         -- Float32 negative
SELECT -1e-7::Float32;         -- Float32 scientific negative

-- Integer-representable floats (fast path)
SELECT '--- Integer-representable floats ---';
SELECT 1.0::Float64;
SELECT -1.0::Float64;
SELECT 42.0::Float64;
SELECT 1000000.0::Float64;
SELECT 1.0::Float32;
SELECT -1.0::Float32;
SELECT 42.0::Float32;
