-- toFloat64 rounds UInt256 max up to 2^256, which is out of range and wraps to 0.
SELECT toUInt256(toFloat64(toUInt256(-1)));
-- The largest Float64 below 2^256 converts exactly.
SELECT toUInt256(1.1579208923731618e77);
