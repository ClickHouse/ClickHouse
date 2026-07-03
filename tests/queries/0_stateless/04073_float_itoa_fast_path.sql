-- Correctness tests for the extended itoa fast path in writeFloatTextFastPath.
-- Covers Float32 exp 24-30 and Float64 exp 53-62, including:
--   - Power-of-2 boundary values (mantissa == 0)
--   - Neighbors of power-of-2 boundaries
--   - Negative values (BFloat16 / negative float)
--   - Values at each exponent level

-- Float32 exp 24 (ULP=2, plain itoa)
SELECT 'Float32 exp 24';
SELECT toString(toFloat32(x)) FROM (SELECT arrayJoin([16777216, 16777218, 33554430, -16777216, -16777218]) AS x);

-- Float32 exp 25 (ULP=4, rounding path)
SELECT 'Float32 exp 25';
SELECT toString(toFloat32(x)) FROM (SELECT arrayJoin([33554432, 33554436, 67108860, -33554432, -33554436]) AS x);

-- Float32 exp 26 (ULP=8)
SELECT 'Float32 exp 26';
SELECT toString(toFloat32(x)) FROM (SELECT arrayJoin([67108864, 67108872, 134217720, -67108864]) AS x);

-- Float32 exp 30 (ULP=128, largest shift)
SELECT 'Float32 exp 30';
SELECT toString(toFloat32(x)) FROM (SELECT arrayJoin([1073741824, 1073741952, 2147483520, -1073741824, -1073741952]) AS x);

-- BFloat16 negative values (goes through Float32 rounding path)
SELECT 'BFloat16 negative';
SELECT toString(toFloat32(toBFloat16(x))) FROM (SELECT arrayJoin([toFloat32(-457179136), toFloat32(-33554432), toFloat32(457179136)]) AS x);

-- Float64 exp 53 (ULP=2, plain itoa)
SELECT 'Float64 exp 53';
SELECT toString(toFloat64(x)) FROM (SELECT arrayJoin([9007199254740992, 9007199254740994, 18014398509481982, -9007199254740992]) AS x);

-- Float64 exp 54 (ULP=4, rounding path)
SELECT 'Float64 exp 54';
SELECT toString(toFloat64(x)) FROM (SELECT arrayJoin([18014398509481984, 18014398509481988, -18014398509481984]) AS x);

-- Float64 exp 55 (ULP=8)
SELECT 'Float64 exp 55';
SELECT toString(toFloat64(x)) FROM (SELECT arrayJoin([36028797018963968, 36028797018963976, -36028797018963968]) AS x);

-- Float64 exp 62 (ULP=1024, requires % 10000)
SELECT 'Float64 exp 62';
SELECT toString(toFloat64(x)) FROM (SELECT arrayJoin([4611686018427387904, 4611686018427388928, -4611686018427387904]) AS x);
