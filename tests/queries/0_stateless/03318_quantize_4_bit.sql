SELECT hex(quantize4Bit(cast([1.0, -1.0] as Array(Float32)), 2));
SELECT hex(quantize4Bit(cast([0.5, -0.5, 0.25, -0.25] as Array(Float32)), 4));
SELECT hex(quantize4Bit(cast([1.0, -1.0] as Array(Float32)), 2));
SELECT hex(quantize4Bit(cast([1.0, 0.0, -1.0] as Array(Float32)), 3));

SELECT hex(quantize4Bit(cast([0.0, 0.0] as Array(Float32)), 2));
SELECT hex(quantize4Bit(cast([0.0, 0.0, 0.0, 0.0] as Array(Float32)), 4));

SELECT hex(quantize4Bit(cast([0.000001, -0.000001] as Array(Float32)), 2));
SELECT hex(quantize4Bit(cast([1e-10, -1e-10, 1e-20, -1e-20] as Array(Float32)), 4));

SELECT hex(quantize4Bit(cast([1e10, -1e10] as Array(Float32)), 2));
SELECT hex(quantize4Bit(cast([1e20, -1e20, 1e30, -1e30] as Array(Float32)), 4));

SELECT hex(quantize4Bit(cast([nan, inf, -inf] as Array(Float32)), 3));
SELECT hex(quantize4Bit(cast([nan, inf, -inf, 1.0] as Array(Float32)), 4));

SELECT hex(quantize4Bit(cast([1.0, 0.1, 0.01, 0.001] as Array(Float32)), 4));
SELECT hex(quantize4Bit(cast([-1.0, -0.1, -0.01, -0.001] as Array(Float32)), 4));

SELECT hex(quantize4Bit(cast([1.0, -1.0, 0.5, -0.5] as Array(Float32)), 4));
SELECT hex(quantize4Bit(cast([0.1, -0.1, 0.01, -0.01] as Array(Float32)), 4));

SELECT hex(quantize4Bit(cast([2.0, 4.0, 8.0, 16.0] as Array(Float32)), 4));
SELECT hex(quantize4Bit(cast([1.0, 0.5, 0.25, 0.125] as Array(Float32)), 4));

SELECT hex(quantize4Bit(cast([1/3, -1/3, 2/3, -2/3] as Array(Float32)), 4));
SELECT hex(quantize4Bit(cast([1/7, -1/7, 1/11, -1/11] as Array(Float32)), 4));

SELECT hex(quantize4Bit(cast([toFloat32(1.0), toFloat32(-1.0)] as Array(Float32)), 2));

SELECT hex(quantize4Bit(cast([1.0] as Array(Float32)), 1));
SELECT hex(quantize4Bit(cast([1.0, -1.0, 0.5, -0.5, 0.25, -0.25, 0.125, -0.125] as Array(Float32)), 8));

SELECT hex(quantize4Bit(cast([1.0, 1.0, 1.0, 1.0] as Array(Float32)), 4));
SELECT hex(quantize4Bit(cast([-1.0, -1.0, -1.0, -1.0] as Array(Float32)), 4));

SELECT hex(quantize4Bit(cast([0.5, 0.25, 0.125, 0.0625] as Array(Float32)), 4));
SELECT hex(quantize4Bit(cast([1.0, 0.999999, 0.500001, 0.499999] as Array(Float32)), 4));
