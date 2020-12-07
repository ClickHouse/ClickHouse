SELECT toInt64(inf);
SELECT toInt128(inf);
SELECT toInt256(inf); -- { serverError 48 }
SELECT toInt64(nan);
SELECT toInt128(nan);
SELECT toInt256(nan); -- { serverError 48 }
SELECT toUInt64(inf);
SELECT toUInt256(inf); -- { serverError 48 }
SELECT toUInt64(nan);
SELECT toUInt256(nan); -- { serverError 48 }
