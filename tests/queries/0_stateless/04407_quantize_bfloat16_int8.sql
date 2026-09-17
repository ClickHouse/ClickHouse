-- quantizeBFloat16ToInt8 / dequantizeInt8ToBFloat16: 256-level Gaussian Lloyd-Max scalar codec.

-- Round trip is idempotent: re-quantizing any cell's reconstruction level returns the same code.
SELECT count() AS mismatches
FROM (SELECT toInt8(number - 128) AS q FROM numbers(256))
WHERE quantizeBFloat16ToInt8(dequantizeInt8ToBFloat16(q)) != q;

-- Sign bit equals the sign of the value.
SELECT quantizeBFloat16ToInt8(toBFloat16(0.7)) >= 0, quantizeBFloat16ToInt8(toBFloat16(-0.7)) < 0;

-- +0 maps to the positive central cell (code 0), so the sign contract holds for zero too.
SELECT quantizeBFloat16ToInt8(toBFloat16(0.0));

-- Documentation example: 0.5 reconstructs to about 0.4961.
SELECT round(toFloat32(dequantizeInt8ToBFloat16(quantizeBFloat16ToInt8(0.5::BFloat16))), 4);

-- Values beyond the codebook saturate to the extreme codes.
SELECT quantizeBFloat16ToInt8(toBFloat16(100.0)), quantizeBFloat16ToInt8(toBFloat16(-100.0));

-- Quantize a vector via arrayMap, then reconstruct.
SELECT arrayMap(x -> quantizeBFloat16ToInt8(x), [0.1, -0.5, 2.0, -3.25]::Array(BFloat16)) AS codes;
SELECT arrayMap(x -> round(toFloat32(dequantizeInt8ToBFloat16(quantizeBFloat16ToInt8(x))), 4),
                [0.1, -0.5, 2.0, -3.25]::Array(BFloat16)) AS reconstructed;

-- Embedded codebook: the top bit of the code (sign) is a 1-bit quantizer.
SELECT arrayMap(x -> quantizeBFloat16ToInt8(x) >= 0, [0.1, -0.5, 2.0, -3.25, 0.01, -0.01]::Array(BFloat16)) AS sign_bit;

-- Type checks.
SELECT quantizeBFloat16ToInt8(1.0::Float32); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dequantizeInt8ToBFloat16(1.0::Float32); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
