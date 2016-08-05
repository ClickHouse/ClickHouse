/* Без дополнительного параметра */

SELECT round(0), ceil(0), floor(0);

SELECT round(toUInt8(13)), ceil(toUInt8(13)), floor(toUInt8(13));
SELECT round(toUInt16(13)), ceil(toUInt16(13)), floor(toUInt16(13));
SELECT round(toUInt32(13)), ceil(toUInt32(13)), floor(toUInt32(13));
SELECT round(toUInt64(13)), ceil(toUInt64(13)), floor(toUInt64(13));
SELECT round(toInt8(13)), ceil(toInt8(13)), floor(toInt8(13));
SELECT round(toInt16(13)), ceil(toInt16(13)), floor(toInt16(13));
SELECT round(toInt32(13)), ceil(toInt32(13)), floor(toInt32(13));
SELECT round(toInt64(13)), ceil(toInt64(13)), floor(toInt64(13));
SELECT round(toFloat32(13)), ceil(toFloat32(13)), floor(toFloat32(13));
SELECT round(toFloat64(13)), ceil(toFloat64(13)), floor(toFloat64(13));

SELECT round(toInt8(-13)), ceil(toInt8(-13)), floor(toInt8(-13));
SELECT round(toInt16(-13)), ceil(toInt16(-13)), floor(toInt16(-13));
SELECT round(toInt32(-13)), ceil(toInt32(-13)), floor(toInt32(-13));
SELECT round(toInt64(-13)), ceil(toInt64(-13)), floor(toInt64(-13));
SELECT round(toFloat32(-13)), ceil(toFloat32(-13)), floor(toFloat32(-13));
SELECT round(toFloat64(-13)), ceil(toFloat64(-13)), floor(toFloat64(-13));

SELECT round(2.7), ceil(2.7), floor(2.7);
SELECT round(2.1), ceil(2,1), floor(2.1);

SELECT round(-2.7), ceil(-2.7), floor(-2.7);
SELECT round(-2.1), ceil(-2,1), floor(-2.1);

/* UInt8 */

SELECT round(toUInt8(13), toUInt8(2)), ceil(toUInt8(13), toUInt8(2)), floor(toUInt8(13), toUInt8(2));
SELECT round(toUInt8(13), toUInt16(2)), ceil(toUInt8(13), toUInt16(2)), floor(toUInt8(13), toUInt16(2));
SELECT round(toUInt8(13), toUInt32(2)), ceil(toUInt8(13), toUInt32(2)), floor(toUInt8(13), toUInt32(2));
SELECT round(toUInt8(13), toUInt64(2)), ceil(toUInt8(13), toUInt64(2)), floor(toUInt8(13), toUInt64(2));
SELECT round(toUInt8(13), toInt8(2)), ceil(toUInt8(13), toInt8(2)), floor(toUInt8(13), toInt8(2));
SELECT round(toUInt8(13), toInt16(2)), ceil(toUInt8(13), toInt16(2)), floor(toUInt8(13), toInt16(2));
SELECT round(toUInt8(13), toInt32(2)), ceil(toUInt8(13), toInt32(2)), floor(toUInt8(13), toInt32(2));
SELECT round(toUInt8(13), toInt64(2)), ceil(toUInt8(13), toInt64(2)), floor(toUInt8(13), toInt64(2));
SELECT round(toUInt8(13), toFloat32(2.1)), ceil(toUInt8(13), toFloat32(2.1)), floor(toUInt8(13), toFloat32(2.1));
SELECT round(toUInt8(13), toFloat64(2.1)), ceil(toUInt8(13), toFloat64(2.1)), floor(toUInt8(13), toFloat64(2.1));

SELECT round(toUInt8(13), toUInt8(1)), ceil(toUInt8(13), toUInt8(1)), floor(toUInt8(13), toUInt8(1));
SELECT round(toUInt8(13), toUInt16(1)), ceil(toUInt8(13), toUInt16(1)), floor(toUInt8(13), toUInt16(1));
SELECT round(toUInt8(13), toUInt32(1)), ceil(toUInt8(13), toUInt32(1)), floor(toUInt8(13), toUInt32(1));
SELECT round(toUInt8(13), toUInt64(1)), ceil(toUInt8(13), toUInt64(1)), floor(toUInt8(13), toUInt64(1));
SELECT round(toUInt8(13), toInt8(1)), ceil(toUInt8(13), toInt8(1)), floor(toUInt8(13), toInt8(1));
SELECT round(toUInt8(13), toInt16(1)), ceil(toUInt8(13), toInt16(1)), floor(toUInt8(13), toInt16(1));
SELECT round(toUInt8(13), toInt32(1)), ceil(toUInt8(13), toInt32(1)), floor(toUInt8(13), toInt32(1));
SELECT round(toUInt8(13), toInt64(1)), ceil(toUInt8(13), toInt64(1)), floor(toUInt8(13), toInt64(1));
SELECT round(toUInt8(13), toFloat32(1.1)), ceil(toUInt8(13), toFloat32(1.1)), floor(toUInt8(13), toFloat32(1.1));
SELECT round(toUInt8(13), toFloat64(1.1)), ceil(toUInt8(13), toFloat64(1.1)), floor(toUInt8(13), toFloat64(1.1));

SELECT round(toUInt8(13), toUInt16(0)), ceil(toUInt8(13), toUInt16(0)), floor(toUInt8(13), toUInt16(0));
SELECT round(toUInt8(13), toUInt32(0)), ceil(toUInt8(13), toUInt32(0)), floor(toUInt8(13), toUInt32(0));
SELECT round(toUInt8(13), toUInt64(0)), ceil(toUInt8(13), toUInt64(0)), floor(toUInt8(13), toUInt64(0));
SELECT round(toUInt8(13), toInt8(0)), ceil(toUInt8(13), toInt8(0)), floor(toUInt8(13), toInt8(0));
SELECT round(toUInt8(13), toInt16(0)), ceil(toUInt8(13), toInt16(0)), floor(toUInt8(13), toInt16(0));
SELECT round(toUInt8(13), toInt32(0)), ceil(toUInt8(13), toInt32(0)), floor(toUInt8(13), toInt32(0));
SELECT round(toUInt8(13), toInt64(0)), ceil(toUInt8(13), toInt64(0)), floor(toUInt8(13), toInt64(0));
SELECT round(toUInt8(13), toFloat32(0.1)), ceil(toUInt8(13), toFloat32(0.1)), floor(toUInt8(13), toFloat32(0.1));
SELECT round(toUInt8(13), toFloat64(0.1)), ceil(toUInt8(13), toFloat64(0.1)), floor(toUInt8(13), toFloat64(0.1));

SELECT round(toUInt8(13), toInt8(-1)), ceil(toUInt8(13), toInt8(-1)), floor(toUInt8(13), toInt8(-1));
SELECT round(toUInt8(13), toInt16(-1)), ceil(toUInt8(13), toInt16(-1)), floor(toUInt8(13), toInt16(-1));
SELECT round(toUInt8(13), toInt32(-1)), ceil(toUInt8(13), toInt32(-1)), floor(toUInt8(13), toInt32(-1));
SELECT round(toUInt8(13), toInt64(-1)), ceil(toUInt8(13), toInt64(-1)), floor(toUInt8(13), toInt64(-1));
SELECT round(toUInt8(13), toFloat32(1.1)), ceil(toUInt8(13), toFloat32(-1.1)), floor(toUInt8(13), toFloat32(-1.1));
SELECT round(toUInt8(13), toFloat64(1.1)), ceil(toUInt8(13), toFloat64(-1.1)), floor(toUInt8(13), toFloat64(-1.1));

SELECT round(toUInt8(13), toInt8(-2)), ceil(toUInt8(13), toInt8(-2)), floor(toUInt8(13), toInt8(-2));
SELECT round(toUInt8(13), toInt16(-2)), ceil(toUInt8(13), toInt16(-2)), floor(toUInt8(13), toInt16(-2));
SELECT round(toUInt8(13), toInt32(-2)), ceil(toUInt8(13), toInt32(-2)), floor(toUInt8(13), toInt32(-2));
SELECT round(toUInt8(13), toInt64(-2)), ceil(toUInt8(13), toInt64(-2)), floor(toUInt8(13), toInt64(-2));
SELECT round(toUInt8(13), toFloat32(-2.1)), ceil(toUInt8(13), toFloat32(-2.1)), floor(toUInt8(13), toFloat32(-2.1));
SELECT round(toUInt8(13), toFloat64(-2.1)), ceil(toUInt8(13), toFloat64(-2.1)), floor(toUInt8(13), toFloat64(-2.1));

/* UInt16 */

SELECT round(toUInt16(13), toUInt8(2)), ceil(toUInt16(13), toUInt8(2)), floor(toUInt16(13), toUInt8(2));
SELECT round(toUInt16(13), toUInt16(2)), ceil(toUInt16(13), toUInt16(2)), floor(toUInt16(13), toUInt16(2));
SELECT round(toUInt16(13), toUInt32(2)), ceil(toUInt16(13), toUInt32(2)), floor(toUInt16(13), toUInt32(2));
SELECT round(toUInt16(13), toUInt64(2)), ceil(toUInt16(13), toUInt64(2)), floor(toUInt16(13), toUInt64(2));
SELECT round(toUInt16(13), toInt8(2)), ceil(toUInt16(13), toInt8(2)), floor(toUInt16(13), toInt8(2));
SELECT round(toUInt16(13), toInt16(2)), ceil(toUInt16(13), toInt16(2)), floor(toUInt16(13), toInt16(2));
SELECT round(toUInt16(13), toInt32(2)), ceil(toUInt16(13), toInt32(2)), floor(toUInt16(13), toInt32(2));
SELECT round(toUInt16(13), toInt64(2)), ceil(toUInt16(13), toInt64(2)), floor(toUInt16(13), toInt64(2));
SELECT round(toUInt16(13), toFloat32(2.1)), ceil(toUInt16(13), toFloat32(2.1)), floor(toUInt16(13), toFloat32(2.1));
SELECT round(toUInt16(13), toFloat64(2.1)), ceil(toUInt16(13), toFloat64(2.1)), floor(toUInt16(13), toFloat64(2.1));

SELECT round(toUInt16(13), toUInt8(1)), ceil(toUInt16(13), toUInt8(1)), floor(toUInt16(13), toUInt8(1));
SELECT round(toUInt16(13), toUInt16(1)), ceil(toUInt16(13), toUInt16(1)), floor(toUInt16(13), toUInt16(1));
SELECT round(toUInt16(13), toUInt32(1)), ceil(toUInt16(13), toUInt32(1)), floor(toUInt16(13), toUInt32(1));
SELECT round(toUInt16(13), toUInt64(1)), ceil(toUInt16(13), toUInt64(1)), floor(toUInt16(13), toUInt64(1));
SELECT round(toUInt16(13), toInt8(1)), ceil(toUInt16(13), toInt8(1)), floor(toUInt16(13), toInt8(1));
SELECT round(toUInt16(13), toInt16(1)), ceil(toUInt16(13), toInt16(1)), floor(toUInt16(13), toInt16(1));
SELECT round(toUInt16(13), toInt32(1)), ceil(toUInt16(13), toInt32(1)), floor(toUInt16(13), toInt32(1));
SELECT round(toUInt16(13), toInt64(1)), ceil(toUInt16(13), toInt64(1)), floor(toUInt16(13), toInt64(1));
SELECT round(toUInt16(13), toFloat32(1.1)), ceil(toUInt16(13), toFloat32(1.1)), floor(toUInt16(13), toFloat32(1.1));
SELECT round(toUInt16(13), toFloat64(1.1)), ceil(toUInt16(13), toFloat64(1.1)), floor(toUInt16(13), toFloat64(1.1));

SELECT round(toUInt16(13), toUInt16(0)), ceil(toUInt16(13), toUInt16(0)), floor(toUInt16(13), toUInt16(0));
SELECT round(toUInt16(13), toUInt32(0)), ceil(toUInt16(13), toUInt32(0)), floor(toUInt16(13), toUInt32(0));
SELECT round(toUInt16(13), toUInt64(0)), ceil(toUInt16(13), toUInt64(0)), floor(toUInt16(13), toUInt64(0));
SELECT round(toUInt16(13), toInt8(0)), ceil(toUInt16(13), toInt8(0)), floor(toUInt16(13), toInt8(0));
SELECT round(toUInt16(13), toInt16(0)), ceil(toUInt16(13), toInt16(0)), floor(toUInt16(13), toInt16(0));
SELECT round(toUInt16(13), toInt32(0)), ceil(toUInt16(13), toInt32(0)), floor(toUInt16(13), toInt32(0));
SELECT round(toUInt16(13), toInt64(0)), ceil(toUInt16(13), toInt64(0)), floor(toUInt16(13), toInt64(0));
SELECT round(toUInt16(13), toFloat32(0.1)), ceil(toUInt16(13), toFloat32(0.1)), floor(toUInt16(13), toFloat32(0.1));
SELECT round(toUInt16(13), toFloat64(0.1)), ceil(toUInt16(13), toFloat64(0.1)), floor(toUInt16(13), toFloat64(0.1));

SELECT round(toUInt16(13), toInt8(-1)), ceil(toUInt16(13), toInt8(-1)), floor(toUInt16(13), toInt8(-1));
SELECT round(toUInt16(13), toInt16(-1)), ceil(toUInt16(13), toInt16(-1)), floor(toUInt16(13), toInt16(-1));
SELECT round(toUInt16(13), toInt32(-1)), ceil(toUInt16(13), toInt32(-1)), floor(toUInt16(13), toInt32(-1));
SELECT round(toUInt16(13), toInt64(-1)), ceil(toUInt16(13), toInt64(-1)), floor(toUInt16(13), toInt64(-1));
SELECT round(toUInt16(13), toFloat32(1.1)), ceil(toUInt16(13), toFloat32(-1.1)), floor(toUInt16(13), toFloat32(-1.1));
SELECT round(toUInt16(13), toFloat64(1.1)), ceil(toUInt16(13), toFloat64(-1.1)), floor(toUInt16(13), toFloat64(-1.1));

SELECT round(toUInt16(13), toInt8(-2)), ceil(toUInt16(13), toInt8(-2)), floor(toUInt16(13), toInt8(-2));
SELECT round(toUInt16(13), toInt16(-2)), ceil(toUInt16(13), toInt16(-2)), floor(toUInt16(13), toInt16(-2));
SELECT round(toUInt16(13), toInt32(-2)), ceil(toUInt16(13), toInt32(-2)), floor(toUInt16(13), toInt32(-2));
SELECT round(toUInt16(13), toInt64(-2)), ceil(toUInt16(13), toInt64(-2)), floor(toUInt16(13), toInt64(-2));
SELECT round(toUInt16(13), toFloat32(-2.1)), ceil(toUInt16(13), toFloat32(-2.1)), floor(toUInt16(13), toFloat32(-2.1));
SELECT round(toUInt16(13), toFloat64(-2.1)), ceil(toUInt16(13), toFloat64(-2.1)), floor(toUInt16(13), toFloat64(-2.1));

/* UInt32 */

SELECT round(toUInt32(13), toUInt8(2)), ceil(toUInt32(13), toUInt8(2)), floor(toUInt32(13), toUInt8(2));
SELECT round(toUInt32(13), toUInt16(2)), ceil(toUInt32(13), toUInt16(2)), floor(toUInt32(13), toUInt16(2));
SELECT round(toUInt32(13), toUInt32(2)), ceil(toUInt32(13), toUInt32(2)), floor(toUInt32(13), toUInt32(2));
SELECT round(toUInt32(13), toUInt64(2)), ceil(toUInt32(13), toUInt64(2)), floor(toUInt32(13), toUInt64(2));
SELECT round(toUInt32(13), toInt8(2)), ceil(toUInt32(13), toInt8(2)), floor(toUInt32(13), toInt8(2));
SELECT round(toUInt32(13), toInt16(2)), ceil(toUInt32(13), toInt16(2)), floor(toUInt32(13), toInt16(2));
SELECT round(toUInt32(13), toInt32(2)), ceil(toUInt32(13), toInt32(2)), floor(toUInt32(13), toInt32(2));
SELECT round(toUInt32(13), toInt64(2)), ceil(toUInt32(13), toInt64(2)), floor(toUInt32(13), toInt64(2));
SELECT round(toUInt32(13), toFloat32(2.1)), ceil(toUInt32(13), toFloat32(2.1)), floor(toUInt32(13), toFloat32(2.1));
SELECT round(toUInt32(13), toFloat64(2.1)), ceil(toUInt32(13), toFloat64(2.1)), floor(toUInt32(13), toFloat64(2.1));

SELECT round(toUInt32(13), toUInt8(1)), ceil(toUInt32(13), toUInt8(1)), floor(toUInt32(13), toUInt8(1));
SELECT round(toUInt32(13), toUInt16(1)), ceil(toUInt32(13), toUInt16(1)), floor(toUInt32(13), toUInt16(1));
SELECT round(toUInt32(13), toUInt32(1)), ceil(toUInt32(13), toUInt32(1)), floor(toUInt32(13), toUInt32(1));
SELECT round(toUInt32(13), toUInt64(1)), ceil(toUInt32(13), toUInt64(1)), floor(toUInt32(13), toUInt64(1));
SELECT round(toUInt32(13), toInt8(1)), ceil(toUInt32(13), toInt8(1)), floor(toUInt32(13), toInt8(1));
SELECT round(toUInt32(13), toInt16(1)), ceil(toUInt32(13), toInt16(1)), floor(toUInt32(13), toInt16(1));
SELECT round(toUInt32(13), toInt32(1)), ceil(toUInt32(13), toInt32(1)), floor(toUInt32(13), toInt32(1));
SELECT round(toUInt32(13), toInt64(1)), ceil(toUInt32(13), toInt64(1)), floor(toUInt32(13), toInt64(1));
SELECT round(toUInt32(13), toFloat32(1.1)), ceil(toUInt32(13), toFloat32(1.1)), floor(toUInt32(13), toFloat32(1.1));
SELECT round(toUInt32(13), toFloat64(1.1)), ceil(toUInt32(13), toFloat64(1.1)), floor(toUInt32(13), toFloat64(1.1));

SELECT round(toUInt32(13), toUInt16(0)), ceil(toUInt32(13), toUInt16(0)), floor(toUInt32(13), toUInt16(0));
SELECT round(toUInt32(13), toUInt32(0)), ceil(toUInt32(13), toUInt32(0)), floor(toUInt32(13), toUInt32(0));
SELECT round(toUInt32(13), toUInt64(0)), ceil(toUInt32(13), toUInt64(0)), floor(toUInt32(13), toUInt64(0));
SELECT round(toUInt32(13), toInt8(0)), ceil(toUInt32(13), toInt8(0)), floor(toUInt32(13), toInt8(0));
SELECT round(toUInt32(13), toInt16(0)), ceil(toUInt32(13), toInt16(0)), floor(toUInt32(13), toInt16(0));
SELECT round(toUInt32(13), toInt32(0)), ceil(toUInt32(13), toInt32(0)), floor(toUInt32(13), toInt32(0));
SELECT round(toUInt32(13), toInt64(0)), ceil(toUInt32(13), toInt64(0)), floor(toUInt32(13), toInt64(0));
SELECT round(toUInt32(13), toFloat32(0.1)), ceil(toUInt32(13), toFloat32(0.1)), floor(toUInt32(13), toFloat32(0.1));
SELECT round(toUInt32(13), toFloat64(0.1)), ceil(toUInt32(13), toFloat64(0.1)), floor(toUInt32(13), toFloat64(0.1));

SELECT round(toUInt32(13), toInt8(-1)), ceil(toUInt32(13), toInt8(-1)), floor(toUInt32(13), toInt8(-1));
SELECT round(toUInt32(13), toInt16(-1)), ceil(toUInt32(13), toInt16(-1)), floor(toUInt32(13), toInt16(-1));
SELECT round(toUInt32(13), toInt32(-1)), ceil(toUInt32(13), toInt32(-1)), floor(toUInt32(13), toInt32(-1));
SELECT round(toUInt32(13), toInt64(-1)), ceil(toUInt32(13), toInt64(-1)), floor(toUInt32(13), toInt64(-1));
SELECT round(toUInt32(13), toFloat32(1.1)), ceil(toUInt32(13), toFloat32(-1.1)), floor(toUInt32(13), toFloat32(-1.1));
SELECT round(toUInt32(13), toFloat64(1.1)), ceil(toUInt32(13), toFloat64(-1.1)), floor(toUInt32(13), toFloat64(-1.1));

SELECT round(toUInt32(13), toInt8(-2)), ceil(toUInt32(13), toInt8(-2)), floor(toUInt32(13), toInt8(-2));
SELECT round(toUInt32(13), toInt16(-2)), ceil(toUInt32(13), toInt16(-2)), floor(toUInt32(13), toInt16(-2));
SELECT round(toUInt32(13), toInt32(-2)), ceil(toUInt32(13), toInt32(-2)), floor(toUInt32(13), toInt32(-2));
SELECT round(toUInt32(13), toInt64(-2)), ceil(toUInt32(13), toInt64(-2)), floor(toUInt32(13), toInt64(-2));
SELECT round(toUInt32(13), toFloat32(-2.1)), ceil(toUInt32(13), toFloat32(-2.1)), floor(toUInt32(13), toFloat32(-2.1));
SELECT round(toUInt32(13), toFloat64(-2.1)), ceil(toUInt32(13), toFloat64(-2.1)), floor(toUInt32(13), toFloat64(-2.1));

/* UInt64 */

SELECT round(toUInt64(13), toUInt8(2)), ceil(toUInt64(13), toUInt8(2)), floor(toUInt64(13), toUInt8(2));
SELECT round(toUInt64(13), toUInt16(2)), ceil(toUInt64(13), toUInt16(2)), floor(toUInt64(13), toUInt16(2));
SELECT round(toUInt64(13), toUInt32(2)), ceil(toUInt64(13), toUInt32(2)), floor(toUInt64(13), toUInt32(2));
SELECT round(toUInt64(13), toUInt64(2)), ceil(toUInt64(13), toUInt64(2)), floor(toUInt64(13), toUInt64(2));
SELECT round(toUInt64(13), toInt8(2)), ceil(toUInt64(13), toInt8(2)), floor(toUInt64(13), toInt8(2));
SELECT round(toUInt64(13), toInt16(2)), ceil(toUInt64(13), toInt16(2)), floor(toUInt64(13), toInt16(2));
SELECT round(toUInt64(13), toInt32(2)), ceil(toUInt64(13), toInt32(2)), floor(toUInt64(13), toInt32(2));
SELECT round(toUInt64(13), toInt64(2)), ceil(toUInt64(13), toInt64(2)), floor(toUInt64(13), toInt64(2));
SELECT round(toUInt64(13), toFloat32(2.1)), ceil(toUInt64(13), toFloat32(2.1)), floor(toUInt64(13), toFloat32(2.1));
SELECT round(toUInt64(13), toFloat64(2.1)), ceil(toUInt64(13), toFloat64(2.1)), floor(toUInt64(13), toFloat64(2.1));

SELECT round(toUInt64(13), toUInt8(1)), ceil(toUInt64(13), toUInt8(1)), floor(toUInt64(13), toUInt8(1));
SELECT round(toUInt64(13), toUInt16(1)), ceil(toUInt64(13), toUInt16(1)), floor(toUInt64(13), toUInt16(1));
SELECT round(toUInt64(13), toUInt32(1)), ceil(toUInt64(13), toUInt32(1)), floor(toUInt64(13), toUInt32(1));
SELECT round(toUInt64(13), toUInt64(1)), ceil(toUInt64(13), toUInt64(1)), floor(toUInt64(13), toUInt64(1));
SELECT round(toUInt64(13), toInt8(1)), ceil(toUInt64(13), toInt8(1)), floor(toUInt64(13), toInt8(1));
SELECT round(toUInt64(13), toInt16(1)), ceil(toUInt64(13), toInt16(1)), floor(toUInt64(13), toInt16(1));
SELECT round(toUInt64(13), toInt32(1)), ceil(toUInt64(13), toInt32(1)), floor(toUInt64(13), toInt32(1));
SELECT round(toUInt64(13), toInt64(1)), ceil(toUInt64(13), toInt64(1)), floor(toUInt64(13), toInt64(1));
SELECT round(toUInt64(13), toFloat32(1.1)), ceil(toUInt64(13), toFloat32(1.1)), floor(toUInt64(13), toFloat32(1.1));
SELECT round(toUInt64(13), toFloat64(1.1)), ceil(toUInt64(13), toFloat64(1.1)), floor(toUInt64(13), toFloat64(1.1));

SELECT round(toUInt64(13), toUInt16(0)), ceil(toUInt64(13), toUInt16(0)), floor(toUInt64(13), toUInt16(0));
SELECT round(toUInt64(13), toUInt32(0)), ceil(toUInt64(13), toUInt32(0)), floor(toUInt64(13), toUInt32(0));
SELECT round(toUInt64(13), toUInt64(0)), ceil(toUInt64(13), toUInt64(0)), floor(toUInt64(13), toUInt64(0));
SELECT round(toUInt64(13), toInt8(0)), ceil(toUInt64(13), toInt8(0)), floor(toUInt64(13), toInt8(0));
SELECT round(toUInt64(13), toInt16(0)), ceil(toUInt64(13), toInt16(0)), floor(toUInt64(13), toInt16(0));
SELECT round(toUInt64(13), toInt32(0)), ceil(toUInt64(13), toInt32(0)), floor(toUInt64(13), toInt32(0));
SELECT round(toUInt64(13), toInt64(0)), ceil(toUInt64(13), toInt64(0)), floor(toUInt64(13), toInt64(0));
SELECT round(toUInt64(13), toFloat32(0.1)), ceil(toUInt64(13), toFloat32(0.1)), floor(toUInt64(13), toFloat32(0.1));
SELECT round(toUInt64(13), toFloat64(0.1)), ceil(toUInt64(13), toFloat64(0.1)), floor(toUInt64(13), toFloat64(0.1));

SELECT round(toUInt64(13), toInt8(-1)), ceil(toUInt64(13), toInt8(-1)), floor(toUInt64(13), toInt8(-1));
SELECT round(toUInt64(13), toInt16(-1)), ceil(toUInt64(13), toInt16(-1)), floor(toUInt64(13), toInt16(-1));
SELECT round(toUInt64(13), toInt32(-1)), ceil(toUInt64(13), toInt32(-1)), floor(toUInt64(13), toInt32(-1));
SELECT round(toUInt64(13), toInt64(-1)), ceil(toUInt64(13), toInt64(-1)), floor(toUInt64(13), toInt64(-1));
SELECT round(toUInt64(13), toFloat32(1.1)), ceil(toUInt64(13), toFloat32(-1.1)), floor(toUInt64(13), toFloat32(-1.1));
SELECT round(toUInt64(13), toFloat64(1.1)), ceil(toUInt64(13), toFloat64(-1.1)), floor(toUInt64(13), toFloat64(-1.1));

SELECT round(toUInt64(13), toInt8(-2)), ceil(toUInt64(13), toInt8(-2)), floor(toUInt64(13), toInt8(-2));
SELECT round(toUInt64(13), toInt16(-2)), ceil(toUInt64(13), toInt16(-2)), floor(toUInt64(13), toInt16(-2));
SELECT round(toUInt64(13), toInt32(-2)), ceil(toUInt64(13), toInt32(-2)), floor(toUInt64(13), toInt32(-2));
SELECT round(toUInt64(13), toInt64(-2)), ceil(toUInt64(13), toInt64(-2)), floor(toUInt64(13), toInt64(-2));
SELECT round(toUInt64(13), toFloat32(-2.1)), ceil(toUInt64(13), toFloat32(-2.1)), floor(toUInt64(13), toFloat32(-2.1));
SELECT round(toUInt64(13), toFloat64(-2.1)), ceil(toUInt64(13), toFloat64(-2.1)), floor(toUInt64(13), toFloat64(-2.1));

/* Int8 */

SELECT round(toInt8(13), toUInt8(2)), ceil(toInt8(13), toUInt8(2)), floor(toInt8(13), toUInt8(2));
SELECT round(toInt8(13), toUInt16(2)), ceil(toInt8(13), toUInt16(2)), floor(toInt8(13), toUInt16(2));
SELECT round(toInt8(13), toUInt32(2)), ceil(toInt8(13), toUInt32(2)), floor(toInt8(13), toUInt32(2));
SELECT round(toInt8(13), toUInt64(2)), ceil(toInt8(13), toUInt64(2)), floor(toInt8(13), toUInt64(2));
SELECT round(toInt8(13), toInt8(2)), ceil(toInt8(13), toInt8(2)), floor(toInt8(13), toInt8(2));
SELECT round(toInt8(13), toInt16(2)), ceil(toInt8(13), toInt16(2)), floor(toInt8(13), toInt16(2));
SELECT round(toInt8(13), toInt32(2)), ceil(toInt8(13), toInt32(2)), floor(toInt8(13), toInt32(2));
SELECT round(toInt8(13), toInt64(2)), ceil(toInt8(13), toInt64(2)), floor(toInt8(13), toInt64(2));
SELECT round(toInt8(13), toFloat32(2.1)), ceil(toInt8(13), toFloat32(2.1)), floor(toInt8(13), toFloat32(2.1));
SELECT round(toInt8(13), toFloat64(2.1)), ceil(toInt8(13), toFloat64(2.1)), floor(toInt8(13), toFloat64(2.1));

SELECT round(toInt8(13), toUInt8(1)), ceil(toInt8(13), toUInt8(1)), floor(toInt8(13), toUInt8(1));
SELECT round(toInt8(13), toUInt16(1)), ceil(toInt8(13), toUInt16(1)), floor(toInt8(13), toUInt16(1));
SELECT round(toInt8(13), toUInt32(1)), ceil(toInt8(13), toUInt32(1)), floor(toInt8(13), toUInt32(1));
SELECT round(toInt8(13), toUInt64(1)), ceil(toInt8(13), toUInt64(1)), floor(toInt8(13), toUInt64(1));
SELECT round(toInt8(13), toInt8(1)), ceil(toInt8(13), toInt8(1)), floor(toInt8(13), toInt8(1));
SELECT round(toInt8(13), toInt16(1)), ceil(toInt8(13), toInt16(1)), floor(toInt8(13), toInt16(1));
SELECT round(toInt8(13), toInt32(1)), ceil(toInt8(13), toInt32(1)), floor(toInt8(13), toInt32(1));
SELECT round(toInt8(13), toInt64(1)), ceil(toInt8(13), toInt64(1)), floor(toInt8(13), toInt64(1));
SELECT round(toInt8(13), toFloat32(1.1)), ceil(toInt8(13), toFloat32(1.1)), floor(toInt8(13), toFloat32(1.1));
SELECT round(toInt8(13), toFloat64(1.1)), ceil(toInt8(13), toFloat64(1.1)), floor(toInt8(13), toFloat64(1.1));

SELECT round(toInt8(13), toUInt16(0)), ceil(toInt8(13), toUInt16(0)), floor(toInt8(13), toUInt16(0));
SELECT round(toInt8(13), toUInt32(0)), ceil(toInt8(13), toUInt32(0)), floor(toInt8(13), toUInt32(0));
SELECT round(toInt8(13), toUInt64(0)), ceil(toInt8(13), toUInt64(0)), floor(toInt8(13), toUInt64(0));
SELECT round(toInt8(13), toInt8(0)), ceil(toInt8(13), toInt8(0)), floor(toInt8(13), toInt8(0));
SELECT round(toInt8(13), toInt16(0)), ceil(toInt8(13), toInt16(0)), floor(toInt8(13), toInt16(0));
SELECT round(toInt8(13), toInt32(0)), ceil(toInt8(13), toInt32(0)), floor(toInt8(13), toInt32(0));
SELECT round(toInt8(13), toInt64(0)), ceil(toInt8(13), toInt64(0)), floor(toInt8(13), toInt64(0));
SELECT round(toInt8(13), toFloat32(0.1)), ceil(toInt8(13), toFloat32(0.1)), floor(toInt8(13), toFloat32(0.1));
SELECT round(toInt8(13), toFloat64(0.1)), ceil(toInt8(13), toFloat64(0.1)), floor(toInt8(13), toFloat64(0.1));

SELECT round(toInt8(13), toInt8(-1)), ceil(toInt8(13), toInt8(-1)), floor(toInt8(13), toInt8(-1));
SELECT round(toInt8(13), toInt16(-1)), ceil(toInt8(13), toInt16(-1)), floor(toInt8(13), toInt16(-1));
SELECT round(toInt8(13), toInt32(-1)), ceil(toInt8(13), toInt32(-1)), floor(toInt8(13), toInt32(-1));
SELECT round(toInt8(13), toInt64(-1)), ceil(toInt8(13), toInt64(-1)), floor(toInt8(13), toInt64(-1));
SELECT round(toInt8(13), toFloat32(1.1)), ceil(toInt8(13), toFloat32(-1.1)), floor(toInt8(13), toFloat32(-1.1));
SELECT round(toInt8(13), toFloat64(1.1)), ceil(toInt8(13), toFloat64(-1.1)), floor(toInt8(13), toFloat64(-1.1));

SELECT round(toInt8(13), toInt8(-2)), ceil(toInt8(13), toInt8(-2)), floor(toInt8(13), toInt8(-2));
SELECT round(toInt8(13), toInt16(-2)), ceil(toInt8(13), toInt16(-2)), floor(toInt8(13), toInt16(-2));
SELECT round(toInt8(13), toInt32(-2)), ceil(toInt8(13), toInt32(-2)), floor(toInt8(13), toInt32(-2));
SELECT round(toInt8(13), toInt64(-2)), ceil(toInt8(13), toInt64(-2)), floor(toInt8(13), toInt64(-2));
SELECT round(toInt8(13), toFloat32(-2.1)), ceil(toInt8(13), toFloat32(-2.1)), floor(toInt8(13), toFloat32(-2.1));
SELECT round(toInt8(13), toFloat64(-2.1)), ceil(toInt8(13), toFloat64(-2.1)), floor(toInt8(13), toFloat64(-2.1));

/* Int16 */

SELECT round(toInt16(13), toUInt8(2)), ceil(toInt16(13), toUInt8(2)), floor(toInt16(13), toUInt8(2));
SELECT round(toInt16(13), toUInt16(2)), ceil(toInt16(13), toUInt16(2)), floor(toInt16(13), toUInt16(2));
SELECT round(toInt16(13), toUInt32(2)), ceil(toInt16(13), toUInt32(2)), floor(toInt16(13), toUInt32(2));
SELECT round(toInt16(13), toUInt64(2)), ceil(toInt16(13), toUInt64(2)), floor(toInt16(13), toUInt64(2));
SELECT round(toInt16(13), toInt8(2)), ceil(toInt16(13), toInt8(2)), floor(toInt16(13), toInt8(2));
SELECT round(toInt16(13), toInt16(2)), ceil(toInt16(13), toInt16(2)), floor(toInt16(13), toInt16(2));
SELECT round(toInt16(13), toInt32(2)), ceil(toInt16(13), toInt32(2)), floor(toInt16(13), toInt32(2));
SELECT round(toInt16(13), toInt64(2)), ceil(toInt16(13), toInt64(2)), floor(toInt16(13), toInt64(2));
SELECT round(toInt16(13), toFloat32(2.1)), ceil(toInt16(13), toFloat32(2.1)), floor(toInt16(13), toFloat32(2.1));
SELECT round(toInt16(13), toFloat64(2.1)), ceil(toInt16(13), toFloat64(2.1)), floor(toInt16(13), toFloat64(2.1));

SELECT round(toInt16(13), toUInt8(1)), ceil(toInt16(13), toUInt8(1)), floor(toInt16(13), toUInt8(1));
SELECT round(toInt16(13), toUInt16(1)), ceil(toInt16(13), toUInt16(1)), floor(toInt16(13), toUInt16(1));
SELECT round(toInt16(13), toUInt32(1)), ceil(toInt16(13), toUInt32(1)), floor(toInt16(13), toUInt32(1));
SELECT round(toInt16(13), toUInt64(1)), ceil(toInt16(13), toUInt64(1)), floor(toInt16(13), toUInt64(1));
SELECT round(toInt16(13), toInt8(1)), ceil(toInt16(13), toInt8(1)), floor(toInt16(13), toInt8(1));
SELECT round(toInt16(13), toInt16(1)), ceil(toInt16(13), toInt16(1)), floor(toInt16(13), toInt16(1));
SELECT round(toInt16(13), toInt32(1)), ceil(toInt16(13), toInt32(1)), floor(toInt16(13), toInt32(1));
SELECT round(toInt16(13), toInt64(1)), ceil(toInt16(13), toInt64(1)), floor(toInt16(13), toInt64(1));
SELECT round(toInt16(13), toFloat32(1.1)), ceil(toInt16(13), toFloat32(1.1)), floor(toInt16(13), toFloat32(1.1));
SELECT round(toInt16(13), toFloat64(1.1)), ceil(toInt16(13), toFloat64(1.1)), floor(toInt16(13), toFloat64(1.1));

SELECT round(toInt16(13), toUInt16(0)), ceil(toInt16(13), toUInt16(0)), floor(toInt16(13), toUInt16(0));
SELECT round(toInt16(13), toUInt32(0)), ceil(toInt16(13), toUInt32(0)), floor(toInt16(13), toUInt32(0));
SELECT round(toInt16(13), toUInt64(0)), ceil(toInt16(13), toUInt64(0)), floor(toInt16(13), toUInt64(0));
SELECT round(toInt16(13), toInt8(0)), ceil(toInt16(13), toInt8(0)), floor(toInt16(13), toInt8(0));
SELECT round(toInt16(13), toInt16(0)), ceil(toInt16(13), toInt16(0)), floor(toInt16(13), toInt16(0));
SELECT round(toInt16(13), toInt32(0)), ceil(toInt16(13), toInt32(0)), floor(toInt16(13), toInt32(0));
SELECT round(toInt16(13), toInt64(0)), ceil(toInt16(13), toInt64(0)), floor(toInt16(13), toInt64(0));
SELECT round(toInt16(13), toFloat32(0.1)), ceil(toInt16(13), toFloat32(0.1)), floor(toInt16(13), toFloat32(0.1));
SELECT round(toInt16(13), toFloat64(0.1)), ceil(toInt16(13), toFloat64(0.1)), floor(toInt16(13), toFloat64(0.1));

SELECT round(toInt16(13), toInt8(-1)), ceil(toInt16(13), toInt8(-1)), floor(toInt16(13), toInt8(-1));
SELECT round(toInt16(13), toInt16(-1)), ceil(toInt16(13), toInt16(-1)), floor(toInt16(13), toInt16(-1));
SELECT round(toInt16(13), toInt32(-1)), ceil(toInt16(13), toInt32(-1)), floor(toInt16(13), toInt32(-1));
SELECT round(toInt16(13), toInt64(-1)), ceil(toInt16(13), toInt64(-1)), floor(toInt16(13), toInt64(-1));
SELECT round(toInt16(13), toFloat32(1.1)), ceil(toInt16(13), toFloat32(-1.1)), floor(toInt16(13), toFloat32(-1.1));
SELECT round(toInt16(13), toFloat64(1.1)), ceil(toInt16(13), toFloat64(-1.1)), floor(toInt16(13), toFloat64(-1.1));

SELECT round(toInt16(13), toInt8(-2)), ceil(toInt16(13), toInt8(-2)), floor(toInt16(13), toInt8(-2));
SELECT round(toInt16(13), toInt16(-2)), ceil(toInt16(13), toInt16(-2)), floor(toInt16(13), toInt16(-2));
SELECT round(toInt16(13), toInt32(-2)), ceil(toInt16(13), toInt32(-2)), floor(toInt16(13), toInt32(-2));
SELECT round(toInt16(13), toInt64(-2)), ceil(toInt16(13), toInt64(-2)), floor(toInt16(13), toInt64(-2));
SELECT round(toInt16(13), toFloat32(-2.1)), ceil(toInt16(13), toFloat32(-2.1)), floor(toInt16(13), toFloat32(-2.1));
SELECT round(toInt16(13), toFloat64(-2.1)), ceil(toInt16(13), toFloat64(-2.1)), floor(toInt16(13), toFloat64(-2.1));

/* Int32 */

SELECT round(toInt32(13), toUInt8(2)), ceil(toInt32(13), toUInt8(2)), floor(toInt32(13), toUInt8(2));
SELECT round(toInt32(13), toUInt16(2)), ceil(toInt32(13), toUInt16(2)), floor(toInt32(13), toUInt16(2));
SELECT round(toInt32(13), toUInt32(2)), ceil(toInt32(13), toUInt32(2)), floor(toInt32(13), toUInt32(2));
SELECT round(toInt32(13), toUInt64(2)), ceil(toInt32(13), toUInt64(2)), floor(toInt32(13), toUInt64(2));
SELECT round(toInt32(13), toInt8(2)), ceil(toInt32(13), toInt8(2)), floor(toInt32(13), toInt8(2));
SELECT round(toInt32(13), toInt16(2)), ceil(toInt32(13), toInt16(2)), floor(toInt32(13), toInt16(2));
SELECT round(toInt32(13), toInt32(2)), ceil(toInt32(13), toInt32(2)), floor(toInt32(13), toInt32(2));
SELECT round(toInt32(13), toInt64(2)), ceil(toInt32(13), toInt64(2)), floor(toInt32(13), toInt64(2));
SELECT round(toInt32(13), toFloat32(2.1)), ceil(toInt32(13), toFloat32(2.1)), floor(toInt32(13), toFloat32(2.1));
SELECT round(toInt32(13), toFloat64(2.1)), ceil(toInt32(13), toFloat64(2.1)), floor(toInt32(13), toFloat64(2.1));

SELECT round(toInt32(13), toUInt8(1)), ceil(toInt32(13), toUInt8(1)), floor(toInt32(13), toUInt8(1));
SELECT round(toInt32(13), toUInt16(1)), ceil(toInt32(13), toUInt16(1)), floor(toInt32(13), toUInt16(1));
SELECT round(toInt32(13), toUInt32(1)), ceil(toInt32(13), toUInt32(1)), floor(toInt32(13), toUInt32(1));
SELECT round(toInt32(13), toUInt64(1)), ceil(toInt32(13), toUInt64(1)), floor(toInt32(13), toUInt64(1));
SELECT round(toInt32(13), toInt8(1)), ceil(toInt32(13), toInt8(1)), floor(toInt32(13), toInt8(1));
SELECT round(toInt32(13), toInt16(1)), ceil(toInt32(13), toInt16(1)), floor(toInt32(13), toInt16(1));
SELECT round(toInt32(13), toInt32(1)), ceil(toInt32(13), toInt32(1)), floor(toInt32(13), toInt32(1));
SELECT round(toInt32(13), toInt64(1)), ceil(toInt32(13), toInt64(1)), floor(toInt32(13), toInt64(1));
SELECT round(toInt32(13), toFloat32(1.1)), ceil(toInt32(13), toFloat32(1.1)), floor(toInt32(13), toFloat32(1.1));
SELECT round(toInt32(13), toFloat64(1.1)), ceil(toInt32(13), toFloat64(1.1)), floor(toInt32(13), toFloat64(1.1));

SELECT round(toInt32(13), toUInt16(0)), ceil(toInt32(13), toUInt16(0)), floor(toInt32(13), toUInt16(0));
SELECT round(toInt32(13), toUInt32(0)), ceil(toInt32(13), toUInt32(0)), floor(toInt32(13), toUInt32(0));
SELECT round(toInt32(13), toUInt64(0)), ceil(toInt32(13), toUInt64(0)), floor(toInt32(13), toUInt64(0));
SELECT round(toInt32(13), toInt8(0)), ceil(toInt32(13), toInt8(0)), floor(toInt32(13), toInt8(0));
SELECT round(toInt32(13), toInt16(0)), ceil(toInt32(13), toInt16(0)), floor(toInt32(13), toInt16(0));
SELECT round(toInt32(13), toInt32(0)), ceil(toInt32(13), toInt32(0)), floor(toInt32(13), toInt32(0));
SELECT round(toInt32(13), toInt64(0)), ceil(toInt32(13), toInt64(0)), floor(toInt32(13), toInt64(0));
SELECT round(toInt32(13), toFloat32(0.1)), ceil(toInt32(13), toFloat32(0.1)), floor(toInt32(13), toFloat32(0.1));
SELECT round(toInt32(13), toFloat64(0.1)), ceil(toInt32(13), toFloat64(0.1)), floor(toInt32(13), toFloat64(0.1));

SELECT round(toInt32(13), toInt8(-1)), ceil(toInt32(13), toInt8(-1)), floor(toInt32(13), toInt8(-1));
SELECT round(toInt32(13), toInt16(-1)), ceil(toInt32(13), toInt16(-1)), floor(toInt32(13), toInt16(-1));
SELECT round(toInt32(13), toInt32(-1)), ceil(toInt32(13), toInt32(-1)), floor(toInt32(13), toInt32(-1));
SELECT round(toInt32(13), toInt64(-1)), ceil(toInt32(13), toInt64(-1)), floor(toInt32(13), toInt64(-1));
SELECT round(toInt32(13), toFloat32(1.1)), ceil(toInt32(13), toFloat32(-1.1)), floor(toInt32(13), toFloat32(-1.1));
SELECT round(toInt32(13), toFloat64(1.1)), ceil(toInt32(13), toFloat64(-1.1)), floor(toInt32(13), toFloat64(-1.1));

SELECT round(toInt32(13), toInt8(-2)), ceil(toInt32(13), toInt8(-2)), floor(toInt32(13), toInt8(-2));
SELECT round(toInt32(13), toInt16(-2)), ceil(toInt32(13), toInt16(-2)), floor(toInt32(13), toInt16(-2));
SELECT round(toInt32(13), toInt32(-2)), ceil(toInt32(13), toInt32(-2)), floor(toInt32(13), toInt32(-2));
SELECT round(toInt32(13), toInt64(-2)), ceil(toInt32(13), toInt64(-2)), floor(toInt32(13), toInt64(-2));
SELECT round(toInt32(13), toFloat32(-2.1)), ceil(toInt32(13), toFloat32(-2.1)), floor(toInt32(13), toFloat32(-2.1));
SELECT round(toInt32(13), toFloat64(-2.1)), ceil(toInt32(13), toFloat64(-2.1)), floor(toInt32(13), toFloat64(-2.1));

/* Int64 */

SELECT round(toInt64(13), toUInt8(2)), ceil(toInt64(13), toUInt8(2)), floor(toInt64(13), toUInt8(2));
SELECT round(toInt64(13), toUInt16(2)), ceil(toInt64(13), toUInt16(2)), floor(toInt64(13), toUInt16(2));
SELECT round(toInt64(13), toUInt32(2)), ceil(toInt64(13), toUInt32(2)), floor(toInt64(13), toUInt32(2));
SELECT round(toInt64(13), toUInt64(2)), ceil(toInt64(13), toUInt64(2)), floor(toInt64(13), toUInt64(2));
SELECT round(toInt64(13), toInt8(2)), ceil(toInt64(13), toInt8(2)), floor(toInt64(13), toInt8(2));
SELECT round(toInt64(13), toInt16(2)), ceil(toInt64(13), toInt16(2)), floor(toInt64(13), toInt16(2));
SELECT round(toInt64(13), toInt32(2)), ceil(toInt64(13), toInt32(2)), floor(toInt64(13), toInt32(2));
SELECT round(toInt64(13), toInt64(2)), ceil(toInt64(13), toInt64(2)), floor(toInt64(13), toInt64(2));
SELECT round(toInt64(13), toFloat32(2.1)), ceil(toInt64(13), toFloat32(2.1)), floor(toInt64(13), toFloat32(2.1));
SELECT round(toInt64(13), toFloat64(2.1)), ceil(toInt64(13), toFloat64(2.1)), floor(toInt64(13), toFloat64(2.1));

SELECT round(toInt64(13), toUInt8(1)), ceil(toInt64(13), toUInt8(1)), floor(toInt64(13), toUInt8(1));
SELECT round(toInt64(13), toUInt16(1)), ceil(toInt64(13), toUInt16(1)), floor(toInt64(13), toUInt16(1));
SELECT round(toInt64(13), toUInt32(1)), ceil(toInt64(13), toUInt32(1)), floor(toInt64(13), toUInt32(1));
SELECT round(toInt64(13), toUInt64(1)), ceil(toInt64(13), toUInt64(1)), floor(toInt64(13), toUInt64(1));
SELECT round(toInt64(13), toInt8(1)), ceil(toInt64(13), toInt8(1)), floor(toInt64(13), toInt8(1));
SELECT round(toInt64(13), toInt16(1)), ceil(toInt64(13), toInt16(1)), floor(toInt64(13), toInt16(1));
SELECT round(toInt64(13), toInt32(1)), ceil(toInt64(13), toInt32(1)), floor(toInt64(13), toInt32(1));
SELECT round(toInt64(13), toInt64(1)), ceil(toInt64(13), toInt64(1)), floor(toInt64(13), toInt64(1));
SELECT round(toInt64(13), toFloat32(1.1)), ceil(toInt64(13), toFloat32(1.1)), floor(toInt64(13), toFloat32(1.1));
SELECT round(toInt64(13), toFloat64(1.1)), ceil(toInt64(13), toFloat64(1.1)), floor(toInt64(13), toFloat64(1.1));

SELECT round(toInt64(13), toUInt16(0)), ceil(toInt64(13), toUInt16(0)), floor(toInt64(13), toUInt16(0));
SELECT round(toInt64(13), toUInt32(0)), ceil(toInt64(13), toUInt32(0)), floor(toInt64(13), toUInt32(0));
SELECT round(toInt64(13), toUInt64(0)), ceil(toInt64(13), toUInt64(0)), floor(toInt64(13), toUInt64(0));
SELECT round(toInt64(13), toInt8(0)), ceil(toInt64(13), toInt8(0)), floor(toInt64(13), toInt8(0));
SELECT round(toInt64(13), toInt16(0)), ceil(toInt64(13), toInt16(0)), floor(toInt64(13), toInt16(0));
SELECT round(toInt64(13), toInt32(0)), ceil(toInt64(13), toInt32(0)), floor(toInt64(13), toInt32(0));
SELECT round(toInt64(13), toInt64(0)), ceil(toInt64(13), toInt64(0)), floor(toInt64(13), toInt64(0));
SELECT round(toInt64(13), toFloat32(0.1)), ceil(toInt64(13), toFloat32(0.1)), floor(toInt64(13), toFloat32(0.1));
SELECT round(toInt64(13), toFloat64(0.1)), ceil(toInt64(13), toFloat64(0.1)), floor(toInt64(13), toFloat64(0.1));

SELECT round(toInt64(13), toInt8(-1)), ceil(toInt64(13), toInt8(-1)), floor(toInt64(13), toInt8(-1));
SELECT round(toInt64(13), toInt16(-1)), ceil(toInt64(13), toInt16(-1)), floor(toInt64(13), toInt16(-1));
SELECT round(toInt64(13), toInt32(-1)), ceil(toInt64(13), toInt32(-1)), floor(toInt64(13), toInt32(-1));
SELECT round(toInt64(13), toInt64(-1)), ceil(toInt64(13), toInt64(-1)), floor(toInt64(13), toInt64(-1));
SELECT round(toInt64(13), toFloat32(1.1)), ceil(toInt64(13), toFloat32(-1.1)), floor(toInt64(13), toFloat32(-1.1));
SELECT round(toInt64(13), toFloat64(1.1)), ceil(toInt64(13), toFloat64(-1.1)), floor(toInt64(13), toFloat64(-1.1));

SELECT round(toInt64(13), toInt8(-2)), ceil(toInt64(13), toInt8(-2)), floor(toInt64(13), toInt8(-2));
SELECT round(toInt64(13), toInt16(-2)), ceil(toInt64(13), toInt16(-2)), floor(toInt64(13), toInt16(-2));
SELECT round(toInt64(13), toInt32(-2)), ceil(toInt64(13), toInt32(-2)), floor(toInt64(13), toInt32(-2));
SELECT round(toInt64(13), toInt64(-2)), ceil(toInt64(13), toInt64(-2)), floor(toInt64(13), toInt64(-2));
SELECT round(toInt64(13), toFloat32(-2.1)), ceil(toInt64(13), toFloat32(-2.1)), floor(toInt64(13), toFloat32(-2.1));
SELECT round(toInt64(13), toFloat64(-2.1)), ceil(toInt64(13), toFloat64(-2.1)), floor(toInt64(13), toFloat64(-2.1));

/* Float32 */

SELECT round(toFloat32(13), toUInt8(2)), ceil(toFloat32(13), toUInt8(2)), floor(toFloat32(13), toUInt8(2));
SELECT round(toFloat32(13), toUInt16(2)), ceil(toFloat32(13), toUInt16(2)), floor(toFloat32(13), toUInt16(2));
SELECT round(toFloat32(13), toUInt32(2)), ceil(toFloat32(13), toUInt32(2)), floor(toFloat32(13), toUInt32(2));
SELECT round(toFloat32(13), toUInt64(2)), ceil(toFloat32(13), toUInt64(2)), floor(toFloat32(13), toUInt64(2));
SELECT round(toFloat32(13), toInt8(2)), ceil(toFloat32(13), toInt8(2)), floor(toFloat32(13), toInt8(2));
SELECT round(toFloat32(13), toInt16(2)), ceil(toFloat32(13), toInt16(2)), floor(toFloat32(13), toInt16(2));
SELECT round(toFloat32(13), toInt32(2)), ceil(toFloat32(13), toInt32(2)), floor(toFloat32(13), toInt32(2));
SELECT round(toFloat32(13), toInt64(2)), ceil(toFloat32(13), toInt64(2)), floor(toFloat32(13), toInt64(2));
SELECT round(toFloat32(13), toFloat32(2.1)), ceil(toFloat32(13), toFloat32(2.1)), floor(toFloat32(13), toFloat32(2.1));
SELECT round(toFloat32(13), toFloat64(2.1)), ceil(toFloat32(13), toFloat64(2.1)), floor(toFloat32(13), toFloat64(2.1));

SELECT round(toFloat32(13), toUInt8(1)), ceil(toFloat32(13), toUInt8(1)), floor(toFloat32(13), toUInt8(1));
SELECT round(toFloat32(13), toUInt16(1)), ceil(toFloat32(13), toUInt16(1)), floor(toFloat32(13), toUInt16(1));
SELECT round(toFloat32(13), toUInt32(1)), ceil(toFloat32(13), toUInt32(1)), floor(toFloat32(13), toUInt32(1));
SELECT round(toFloat32(13), toUInt64(1)), ceil(toFloat32(13), toUInt64(1)), floor(toFloat32(13), toUInt64(1));
SELECT round(toFloat32(13), toInt8(1)), ceil(toFloat32(13), toInt8(1)), floor(toFloat32(13), toInt8(1));
SELECT round(toFloat32(13), toInt16(1)), ceil(toFloat32(13), toInt16(1)), floor(toFloat32(13), toInt16(1));
SELECT round(toFloat32(13), toInt32(1)), ceil(toFloat32(13), toInt32(1)), floor(toFloat32(13), toInt32(1));
SELECT round(toFloat32(13), toInt64(1)), ceil(toFloat32(13), toInt64(1)), floor(toFloat32(13), toInt64(1));
SELECT round(toFloat32(13), toFloat32(1.1)), ceil(toFloat32(13), toFloat32(1.1)), floor(toFloat32(13), toFloat32(1.1));
SELECT round(toFloat32(13), toFloat64(1.1)), ceil(toFloat32(13), toFloat64(1.1)), floor(toFloat32(13), toFloat64(1.1));

SELECT round(toFloat32(13), toUInt16(0)), ceil(toFloat32(13), toUInt16(0)), floor(toFloat32(13), toUInt16(0));
SELECT round(toFloat32(13), toUInt32(0)), ceil(toFloat32(13), toUInt32(0)), floor(toFloat32(13), toUInt32(0));
SELECT round(toFloat32(13), toUInt64(0)), ceil(toFloat32(13), toUInt64(0)), floor(toFloat32(13), toUInt64(0));
SELECT round(toFloat32(13), toInt8(0)), ceil(toFloat32(13), toInt8(0)), floor(toFloat32(13), toInt8(0));
SELECT round(toFloat32(13), toInt16(0)), ceil(toFloat32(13), toInt16(0)), floor(toFloat32(13), toInt16(0));
SELECT round(toFloat32(13), toInt32(0)), ceil(toFloat32(13), toInt32(0)), floor(toFloat32(13), toInt32(0));
SELECT round(toFloat32(13), toInt64(0)), ceil(toFloat32(13), toInt64(0)), floor(toFloat32(13), toInt64(0));
SELECT round(toFloat32(13), toFloat32(0.1)), ceil(toFloat32(13), toFloat32(0.1)), floor(toFloat32(13), toFloat32(0.1));
SELECT round(toFloat32(13), toFloat64(0.1)), ceil(toFloat32(13), toFloat64(0.1)), floor(toFloat32(13), toFloat64(0.1));

SELECT round(toFloat32(13), toInt8(-1)), ceil(toFloat32(13), toInt8(-1)), floor(toFloat32(13), toInt8(-1));
SELECT round(toFloat32(13), toInt16(-1)), ceil(toFloat32(13), toInt16(-1)), floor(toFloat32(13), toInt16(-1));
SELECT round(toFloat32(13), toInt32(-1)), ceil(toFloat32(13), toInt32(-1)), floor(toFloat32(13), toInt32(-1));
SELECT round(toFloat32(13), toInt64(-1)), ceil(toFloat32(13), toInt64(-1)), floor(toFloat32(13), toInt64(-1));
SELECT round(toFloat32(13), toFloat32(1.1)), ceil(toFloat32(13), toFloat32(-1.1)), floor(toFloat32(13), toFloat32(-1.1));
SELECT round(toFloat32(13), toFloat64(1.1)), ceil(toFloat32(13), toFloat64(-1.1)), floor(toFloat32(13), toFloat64(-1.1));

SELECT round(toFloat32(13), toInt8(-2)), ceil(toFloat32(13), toInt8(-2)), floor(toFloat32(13), toInt8(-2));
SELECT round(toFloat32(13), toInt16(-2)), ceil(toFloat32(13), toInt16(-2)), floor(toFloat32(13), toInt16(-2));
SELECT round(toFloat32(13), toInt32(-2)), ceil(toFloat32(13), toInt32(-2)), floor(toFloat32(13), toInt32(-2));
SELECT round(toFloat32(13), toInt64(-2)), ceil(toFloat32(13), toInt64(-2)), floor(toFloat32(13), toInt64(-2));
SELECT round(toFloat32(13), toFloat32(-2.1)), ceil(toFloat32(13), toFloat32(-2.1)), floor(toFloat32(13), toFloat32(-2.1));
SELECT round(toFloat32(13), toFloat64(-2.1)), ceil(toFloat32(13), toFloat64(-2.1)), floor(toFloat32(13), toFloat64(-2.1));

/* Float64 */

SELECT round(toFloat64(13), toUInt8(2)), ceil(toFloat64(13), toUInt8(2)), floor(toFloat64(13), toUInt8(2));
SELECT round(toFloat64(13), toUInt16(2)), ceil(toFloat64(13), toUInt16(2)), floor(toFloat64(13), toUInt16(2));
SELECT round(toFloat64(13), toUInt32(2)), ceil(toFloat64(13), toUInt32(2)), floor(toFloat64(13), toUInt32(2));
SELECT round(toFloat64(13), toUInt64(2)), ceil(toFloat64(13), toUInt64(2)), floor(toFloat64(13), toUInt64(2));
SELECT round(toFloat64(13), toInt8(2)), ceil(toFloat64(13), toInt8(2)), floor(toFloat64(13), toInt8(2));
SELECT round(toFloat64(13), toInt16(2)), ceil(toFloat64(13), toInt16(2)), floor(toFloat64(13), toInt16(2));
SELECT round(toFloat64(13), toInt32(2)), ceil(toFloat64(13), toInt32(2)), floor(toFloat64(13), toInt32(2));
SELECT round(toFloat64(13), toInt64(2)), ceil(toFloat64(13), toInt64(2)), floor(toFloat64(13), toInt64(2));
SELECT round(toFloat64(13), toFloat32(2.1)), ceil(toFloat64(13), toFloat32(2.1)), floor(toFloat64(13), toFloat32(2.1));
SELECT round(toFloat64(13), toFloat64(2.1)), ceil(toFloat64(13), toFloat64(2.1)), floor(toFloat64(13), toFloat64(2.1));

SELECT round(toFloat64(13), toUInt8(1)), ceil(toFloat64(13), toUInt8(1)), floor(toFloat64(13), toUInt8(1));
SELECT round(toFloat64(13), toUInt16(1)), ceil(toFloat64(13), toUInt16(1)), floor(toFloat64(13), toUInt16(1));
SELECT round(toFloat64(13), toUInt32(1)), ceil(toFloat64(13), toUInt32(1)), floor(toFloat64(13), toUInt32(1));
SELECT round(toFloat64(13), toUInt64(1)), ceil(toFloat64(13), toUInt64(1)), floor(toFloat64(13), toUInt64(1));
SELECT round(toFloat64(13), toInt8(1)), ceil(toFloat64(13), toInt8(1)), floor(toFloat64(13), toInt8(1));
SELECT round(toFloat64(13), toInt16(1)), ceil(toFloat64(13), toInt16(1)), floor(toFloat64(13), toInt16(1));
SELECT round(toFloat64(13), toInt32(1)), ceil(toFloat64(13), toInt32(1)), floor(toFloat64(13), toInt32(1));
SELECT round(toFloat64(13), toInt64(1)), ceil(toFloat64(13), toInt64(1)), floor(toFloat64(13), toInt64(1));
SELECT round(toFloat64(13), toFloat32(1.1)), ceil(toFloat64(13), toFloat32(1.1)), floor(toFloat64(13), toFloat32(1.1));
SELECT round(toFloat64(13), toFloat64(1.1)), ceil(toFloat64(13), toFloat64(1.1)), floor(toFloat64(13), toFloat64(1.1));

SELECT round(toFloat64(13), toUInt16(0)), ceil(toFloat64(13), toUInt16(0)), floor(toFloat64(13), toUInt16(0));
SELECT round(toFloat64(13), toUInt32(0)), ceil(toFloat64(13), toUInt32(0)), floor(toFloat64(13), toUInt32(0));
SELECT round(toFloat64(13), toUInt64(0)), ceil(toFloat64(13), toUInt64(0)), floor(toFloat64(13), toUInt64(0));
SELECT round(toFloat64(13), toInt8(0)), ceil(toFloat64(13), toInt8(0)), floor(toFloat64(13), toInt8(0));
SELECT round(toFloat64(13), toInt16(0)), ceil(toFloat64(13), toInt16(0)), floor(toFloat64(13), toInt16(0));
SELECT round(toFloat64(13), toInt32(0)), ceil(toFloat64(13), toInt32(0)), floor(toFloat64(13), toInt32(0));
SELECT round(toFloat64(13), toInt64(0)), ceil(toFloat64(13), toInt64(0)), floor(toFloat64(13), toInt64(0));
SELECT round(toFloat64(13), toFloat32(0.1)), ceil(toFloat64(13), toFloat32(0.1)), floor(toFloat64(13), toFloat32(0.1));
SELECT round(toFloat64(13), toFloat64(0.1)), ceil(toFloat64(13), toFloat64(0.1)), floor(toFloat64(13), toFloat64(0.1));

SELECT round(toFloat64(13), toInt8(-1)), ceil(toFloat64(13), toInt8(-1)), floor(toFloat64(13), toInt8(-1));
SELECT round(toFloat64(13), toInt16(-1)), ceil(toFloat64(13), toInt16(-1)), floor(toFloat64(13), toInt16(-1));
SELECT round(toFloat64(13), toInt32(-1)), ceil(toFloat64(13), toInt32(-1)), floor(toFloat64(13), toInt32(-1));
SELECT round(toFloat64(13), toInt64(-1)), ceil(toFloat64(13), toInt64(-1)), floor(toFloat64(13), toInt64(-1));
SELECT round(toFloat64(13), toFloat32(1.1)), ceil(toFloat64(13), toFloat32(-1.1)), floor(toFloat64(13), toFloat32(-1.1));
SELECT round(toFloat64(13), toFloat64(1.1)), ceil(toFloat64(13), toFloat64(-1.1)), floor(toFloat64(13), toFloat64(-1.1));

SELECT round(toFloat64(13), toInt8(-2)), ceil(toFloat64(13), toInt8(-2)), floor(toFloat64(13), toInt8(-2));
SELECT round(toFloat64(13), toInt16(-2)), ceil(toFloat64(13), toInt16(-2)), floor(toFloat64(13), toInt16(-2));
SELECT round(toFloat64(13), toInt32(-2)), ceil(toFloat64(13), toInt32(-2)), floor(toFloat64(13), toInt32(-2));
SELECT round(toFloat64(13), toInt64(-2)), ceil(toFloat64(13), toInt64(-2)), floor(toFloat64(13), toInt64(-2));
SELECT round(toFloat64(13), toFloat32(-2.1)), ceil(toFloat64(13), toFloat32(-2.1)), floor(toFloat64(13), toFloat32(-2.1));
SELECT round(toFloat64(13), toFloat64(-2.1)), ceil(toFloat64(13), toFloat64(-2.1)), floor(toFloat64(13), toFloat64(-2.1));

/* Отрицательное значение */

/* Int8 */

SELECT round(toInt8(-13), toUInt8(2)), ceil(toInt8(-13), toUInt8(2)), floor(toInt8(-13), toUInt8(2));
SELECT round(toInt8(-13), toUInt16(2)), ceil(toInt8(-13), toUInt16(2)), floor(toInt8(-13), toUInt16(2));
SELECT round(toInt8(-13), toUInt32(2)), ceil(toInt8(-13), toUInt32(2)), floor(toInt8(-13), toUInt32(2));
SELECT round(toInt8(-13), toUInt64(2)), ceil(toInt8(-13), toUInt64(2)), floor(toInt8(-13), toUInt64(2));
SELECT round(toInt8(-13), toInt8(2)), ceil(toInt8(-13), toInt8(2)), floor(toInt8(-13), toInt8(2));
SELECT round(toInt8(-13), toInt16(2)), ceil(toInt8(-13), toInt16(2)), floor(toInt8(-13), toInt16(2));
SELECT round(toInt8(-13), toInt32(2)), ceil(toInt8(-13), toInt32(2)), floor(toInt8(-13), toInt32(2));
SELECT round(toInt8(-13), toInt64(2)), ceil(toInt8(-13), toInt64(2)), floor(toInt8(-13), toInt64(2));
SELECT round(toInt8(-13), toFloat32(2.1)), ceil(toInt8(-13), toFloat32(2.1)), floor(toInt8(-13), toFloat32(2.1));
SELECT round(toInt8(-13), toFloat64(2.1)), ceil(toInt8(-13), toFloat64(2.1)), floor(toInt8(-13), toFloat64(2.1));

SELECT round(toInt8(-13), toUInt8(1)), ceil(toInt8(-13), toUInt8(1)), floor(toInt8(-13), toUInt8(1));
SELECT round(toInt8(-13), toUInt16(1)), ceil(toInt8(-13), toUInt16(1)), floor(toInt8(-13), toUInt16(1));
SELECT round(toInt8(-13), toUInt32(1)), ceil(toInt8(-13), toUInt32(1)), floor(toInt8(-13), toUInt32(1));
SELECT round(toInt8(-13), toUInt64(1)), ceil(toInt8(-13), toUInt64(1)), floor(toInt8(-13), toUInt64(1));
SELECT round(toInt8(-13), toInt8(1)), ceil(toInt8(-13), toInt8(1)), floor(toInt8(-13), toInt8(1));
SELECT round(toInt8(-13), toInt16(1)), ceil(toInt8(-13), toInt16(1)), floor(toInt8(-13), toInt16(1));
SELECT round(toInt8(-13), toInt32(1)), ceil(toInt8(-13), toInt32(1)), floor(toInt8(-13), toInt32(1));
SELECT round(toInt8(-13), toInt64(1)), ceil(toInt8(-13), toInt64(1)), floor(toInt8(-13), toInt64(1));
SELECT round(toInt8(-13), toFloat32(1.1)), ceil(toInt8(-13), toFloat32(1.1)), floor(toInt8(-13), toFloat32(1.1));
SELECT round(toInt8(-13), toFloat64(1.1)), ceil(toInt8(-13), toFloat64(1.1)), floor(toInt8(-13), toFloat64(1.1));

SELECT round(toInt8(-13), toUInt16(0)), ceil(toInt8(-13), toUInt16(0)), floor(toInt8(-13), toUInt16(0));
SELECT round(toInt8(-13), toUInt32(0)), ceil(toInt8(-13), toUInt32(0)), floor(toInt8(-13), toUInt32(0));
SELECT round(toInt8(-13), toUInt64(0)), ceil(toInt8(-13), toUInt64(0)), floor(toInt8(-13), toUInt64(0));
SELECT round(toInt8(-13), toInt8(0)), ceil(toInt8(-13), toInt8(0)), floor(toInt8(-13), toInt8(0));
SELECT round(toInt8(-13), toInt16(0)), ceil(toInt8(-13), toInt16(0)), floor(toInt8(-13), toInt16(0));
SELECT round(toInt8(-13), toInt32(0)), ceil(toInt8(-13), toInt32(0)), floor(toInt8(-13), toInt32(0));
SELECT round(toInt8(-13), toInt64(0)), ceil(toInt8(-13), toInt64(0)), floor(toInt8(-13), toInt64(0));
SELECT round(toInt8(-13), toFloat32(0.1)), ceil(toInt8(-13), toFloat32(0.1)), floor(toInt8(-13), toFloat32(0.1));
SELECT round(toInt8(-13), toFloat64(0.1)), ceil(toInt8(-13), toFloat64(0.1)), floor(toInt8(-13), toFloat64(0.1));

SELECT round(toInt8(-13), toInt8(-1)), ceil(toInt8(-13), toInt8(-1)), floor(toInt8(-13), toInt8(-1));
SELECT round(toInt8(-13), toInt16(-1)), ceil(toInt8(-13), toInt16(-1)), floor(toInt8(-13), toInt16(-1));
SELECT round(toInt8(-13), toInt32(-1)), ceil(toInt8(-13), toInt32(-1)), floor(toInt8(-13), toInt32(-1));
SELECT round(toInt8(-13), toInt64(-1)), ceil(toInt8(-13), toInt64(-1)), floor(toInt8(-13), toInt64(-1));
SELECT round(toInt8(-13), toFloat32(1.1)), ceil(toInt8(-13), toFloat32(-1.1)), floor(toInt8(-13), toFloat32(-1.1));
SELECT round(toInt8(-13), toFloat64(1.1)), ceil(toInt8(-13), toFloat64(-1.1)), floor(toInt8(-13), toFloat64(-1.1));

SELECT round(toInt8(-13), toInt8(-2)), ceil(toInt8(-13), toInt8(-2)), floor(toInt8(-13), toInt8(-2));
SELECT round(toInt8(-13), toInt16(-2)), ceil(toInt8(-13), toInt16(-2)), floor(toInt8(-13), toInt16(-2));
SELECT round(toInt8(-13), toInt32(-2)), ceil(toInt8(-13), toInt32(-2)), floor(toInt8(-13), toInt32(-2));
SELECT round(toInt8(-13), toInt64(-2)), ceil(toInt8(-13), toInt64(-2)), floor(toInt8(-13), toInt64(-2));
SELECT round(toInt8(-13), toFloat32(-2.1)), ceil(toInt8(-13), toFloat32(-2.1)), floor(toInt8(-13), toFloat32(-2.1));
SELECT round(toInt8(-13), toFloat64(-2.1)), ceil(toInt8(-13), toFloat64(-2.1)), floor(toInt8(-13), toFloat64(-2.1));

/* Int16 */

SELECT round(toInt16(-13), toUInt8(2)), ceil(toInt16(-13), toUInt8(2)), floor(toInt16(-13), toUInt8(2));
SELECT round(toInt16(-13), toUInt16(2)), ceil(toInt16(-13), toUInt16(2)), floor(toInt16(-13), toUInt16(2));
SELECT round(toInt16(-13), toUInt32(2)), ceil(toInt16(-13), toUInt32(2)), floor(toInt16(-13), toUInt32(2));
SELECT round(toInt16(-13), toUInt64(2)), ceil(toInt16(-13), toUInt64(2)), floor(toInt16(-13), toUInt64(2));
SELECT round(toInt16(-13), toInt8(2)), ceil(toInt16(-13), toInt8(2)), floor(toInt16(-13), toInt8(2));
SELECT round(toInt16(-13), toInt16(2)), ceil(toInt16(-13), toInt16(2)), floor(toInt16(-13), toInt16(2));
SELECT round(toInt16(-13), toInt32(2)), ceil(toInt16(-13), toInt32(2)), floor(toInt16(-13), toInt32(2));
SELECT round(toInt16(-13), toInt64(2)), ceil(toInt16(-13), toInt64(2)), floor(toInt16(-13), toInt64(2));
SELECT round(toInt16(-13), toFloat32(2.1)), ceil(toInt16(-13), toFloat32(2.1)), floor(toInt16(-13), toFloat32(2.1));
SELECT round(toInt16(-13), toFloat64(2.1)), ceil(toInt16(-13), toFloat64(2.1)), floor(toInt16(-13), toFloat64(2.1));

SELECT round(toInt16(-13), toUInt8(1)), ceil(toInt16(-13), toUInt8(1)), floor(toInt16(-13), toUInt8(1));
SELECT round(toInt16(-13), toUInt16(1)), ceil(toInt16(-13), toUInt16(1)), floor(toInt16(-13), toUInt16(1));
SELECT round(toInt16(-13), toUInt32(1)), ceil(toInt16(-13), toUInt32(1)), floor(toInt16(-13), toUInt32(1));
SELECT round(toInt16(-13), toUInt64(1)), ceil(toInt16(-13), toUInt64(1)), floor(toInt16(-13), toUInt64(1));
SELECT round(toInt16(-13), toInt8(1)), ceil(toInt16(-13), toInt8(1)), floor(toInt16(-13), toInt8(1));
SELECT round(toInt16(-13), toInt16(1)), ceil(toInt16(-13), toInt16(1)), floor(toInt16(-13), toInt16(1));
SELECT round(toInt16(-13), toInt32(1)), ceil(toInt16(-13), toInt32(1)), floor(toInt16(-13), toInt32(1));
SELECT round(toInt16(-13), toInt64(1)), ceil(toInt16(-13), toInt64(1)), floor(toInt16(-13), toInt64(1));
SELECT round(toInt16(-13), toFloat32(1.1)), ceil(toInt16(-13), toFloat32(1.1)), floor(toInt16(-13), toFloat32(1.1));
SELECT round(toInt16(-13), toFloat64(1.1)), ceil(toInt16(-13), toFloat64(1.1)), floor(toInt16(-13), toFloat64(1.1));

SELECT round(toInt16(-13), toUInt16(0)), ceil(toInt16(-13), toUInt16(0)), floor(toInt16(-13), toUInt16(0));
SELECT round(toInt16(-13), toUInt32(0)), ceil(toInt16(-13), toUInt32(0)), floor(toInt16(-13), toUInt32(0));
SELECT round(toInt16(-13), toUInt64(0)), ceil(toInt16(-13), toUInt64(0)), floor(toInt16(-13), toUInt64(0));
SELECT round(toInt16(-13), toInt8(0)), ceil(toInt16(-13), toInt8(0)), floor(toInt16(-13), toInt8(0));
SELECT round(toInt16(-13), toInt16(0)), ceil(toInt16(-13), toInt16(0)), floor(toInt16(-13), toInt16(0));
SELECT round(toInt16(-13), toInt32(0)), ceil(toInt16(-13), toInt32(0)), floor(toInt16(-13), toInt32(0));
SELECT round(toInt16(-13), toInt64(0)), ceil(toInt16(-13), toInt64(0)), floor(toInt16(-13), toInt64(0));
SELECT round(toInt16(-13), toFloat32(0.1)), ceil(toInt16(-13), toFloat32(0.1)), floor(toInt16(-13), toFloat32(0.1));
SELECT round(toInt16(-13), toFloat64(0.1)), ceil(toInt16(-13), toFloat64(0.1)), floor(toInt16(-13), toFloat64(0.1));

SELECT round(toInt16(-13), toInt8(-1)), ceil(toInt16(-13), toInt8(-1)), floor(toInt16(-13), toInt8(-1));
SELECT round(toInt16(-13), toInt16(-1)), ceil(toInt16(-13), toInt16(-1)), floor(toInt16(-13), toInt16(-1));
SELECT round(toInt16(-13), toInt32(-1)), ceil(toInt16(-13), toInt32(-1)), floor(toInt16(-13), toInt32(-1));
SELECT round(toInt16(-13), toInt64(-1)), ceil(toInt16(-13), toInt64(-1)), floor(toInt16(-13), toInt64(-1));
SELECT round(toInt16(-13), toFloat32(1.1)), ceil(toInt16(-13), toFloat32(-1.1)), floor(toInt16(-13), toFloat32(-1.1));
SELECT round(toInt16(-13), toFloat64(1.1)), ceil(toInt16(-13), toFloat64(-1.1)), floor(toInt16(-13), toFloat64(-1.1));

SELECT round(toInt16(-13), toInt8(-2)), ceil(toInt16(-13), toInt8(-2)), floor(toInt16(-13), toInt8(-2));
SELECT round(toInt16(-13), toInt16(-2)), ceil(toInt16(-13), toInt16(-2)), floor(toInt16(-13), toInt16(-2));
SELECT round(toInt16(-13), toInt32(-2)), ceil(toInt16(-13), toInt32(-2)), floor(toInt16(-13), toInt32(-2));
SELECT round(toInt16(-13), toInt64(-2)), ceil(toInt16(-13), toInt64(-2)), floor(toInt16(-13), toInt64(-2));
SELECT round(toInt16(-13), toFloat32(-2.1)), ceil(toInt16(-13), toFloat32(-2.1)), floor(toInt16(-13), toFloat32(-2.1));
SELECT round(toInt16(-13), toFloat64(-2.1)), ceil(toInt16(-13), toFloat64(-2.1)), floor(toInt16(-13), toFloat64(-2.1));

/* Int32 */

SELECT round(toInt32(-13), toUInt8(2)), ceil(toInt32(-13), toUInt8(2)), floor(toInt32(-13), toUInt8(2));
SELECT round(toInt32(-13), toUInt16(2)), ceil(toInt32(-13), toUInt16(2)), floor(toInt32(-13), toUInt16(2));
SELECT round(toInt32(-13), toUInt32(2)), ceil(toInt32(-13), toUInt32(2)), floor(toInt32(-13), toUInt32(2));
SELECT round(toInt32(-13), toUInt64(2)), ceil(toInt32(-13), toUInt64(2)), floor(toInt32(-13), toUInt64(2));
SELECT round(toInt32(-13), toInt8(2)), ceil(toInt32(-13), toInt8(2)), floor(toInt32(-13), toInt8(2));
SELECT round(toInt32(-13), toInt16(2)), ceil(toInt32(-13), toInt16(2)), floor(toInt32(-13), toInt16(2));
SELECT round(toInt32(-13), toInt32(2)), ceil(toInt32(-13), toInt32(2)), floor(toInt32(-13), toInt32(2));
SELECT round(toInt32(-13), toInt64(2)), ceil(toInt32(-13), toInt64(2)), floor(toInt32(-13), toInt64(2));
SELECT round(toInt32(-13), toFloat32(2.1)), ceil(toInt32(-13), toFloat32(2.1)), floor(toInt32(-13), toFloat32(2.1));
SELECT round(toInt32(-13), toFloat64(2.1)), ceil(toInt32(-13), toFloat64(2.1)), floor(toInt32(-13), toFloat64(2.1));

SELECT round(toInt32(-13), toUInt8(1)), ceil(toInt32(-13), toUInt8(1)), floor(toInt32(-13), toUInt8(1));
SELECT round(toInt32(-13), toUInt16(1)), ceil(toInt32(-13), toUInt16(1)), floor(toInt32(-13), toUInt16(1));
SELECT round(toInt32(-13), toUInt32(1)), ceil(toInt32(-13), toUInt32(1)), floor(toInt32(-13), toUInt32(1));
SELECT round(toInt32(-13), toUInt64(1)), ceil(toInt32(-13), toUInt64(1)), floor(toInt32(-13), toUInt64(1));
SELECT round(toInt32(-13), toInt8(1)), ceil(toInt32(-13), toInt8(1)), floor(toInt32(-13), toInt8(1));
SELECT round(toInt32(-13), toInt16(1)), ceil(toInt32(-13), toInt16(1)), floor(toInt32(-13), toInt16(1));
SELECT round(toInt32(-13), toInt32(1)), ceil(toInt32(-13), toInt32(1)), floor(toInt32(-13), toInt32(1));
SELECT round(toInt32(-13), toInt64(1)), ceil(toInt32(-13), toInt64(1)), floor(toInt32(-13), toInt64(1));
SELECT round(toInt32(-13), toFloat32(1.1)), ceil(toInt32(-13), toFloat32(1.1)), floor(toInt32(-13), toFloat32(1.1));
SELECT round(toInt32(-13), toFloat64(1.1)), ceil(toInt32(-13), toFloat64(1.1)), floor(toInt32(-13), toFloat64(1.1));

SELECT round(toInt32(-13), toUInt16(0)), ceil(toInt32(-13), toUInt16(0)), floor(toInt32(-13), toUInt16(0));
SELECT round(toInt32(-13), toUInt32(0)), ceil(toInt32(-13), toUInt32(0)), floor(toInt32(-13), toUInt32(0));
SELECT round(toInt32(-13), toUInt64(0)), ceil(toInt32(-13), toUInt64(0)), floor(toInt32(-13), toUInt64(0));
SELECT round(toInt32(-13), toInt8(0)), ceil(toInt32(-13), toInt8(0)), floor(toInt32(-13), toInt8(0));
SELECT round(toInt32(-13), toInt16(0)), ceil(toInt32(-13), toInt16(0)), floor(toInt32(-13), toInt16(0));
SELECT round(toInt32(-13), toInt32(0)), ceil(toInt32(-13), toInt32(0)), floor(toInt32(-13), toInt32(0));
SELECT round(toInt32(-13), toInt64(0)), ceil(toInt32(-13), toInt64(0)), floor(toInt32(-13), toInt64(0));
SELECT round(toInt32(-13), toFloat32(0.1)), ceil(toInt32(-13), toFloat32(0.1)), floor(toInt32(-13), toFloat32(0.1));
SELECT round(toInt32(-13), toFloat64(0.1)), ceil(toInt32(-13), toFloat64(0.1)), floor(toInt32(-13), toFloat64(0.1));

SELECT round(toInt32(-13), toInt8(-1)), ceil(toInt32(-13), toInt8(-1)), floor(toInt32(-13), toInt8(-1));
SELECT round(toInt32(-13), toInt16(-1)), ceil(toInt32(-13), toInt16(-1)), floor(toInt32(-13), toInt16(-1));
SELECT round(toInt32(-13), toInt32(-1)), ceil(toInt32(-13), toInt32(-1)), floor(toInt32(-13), toInt32(-1));
SELECT round(toInt32(-13), toInt64(-1)), ceil(toInt32(-13), toInt64(-1)), floor(toInt32(-13), toInt64(-1));
SELECT round(toInt32(-13), toFloat32(1.1)), ceil(toInt32(-13), toFloat32(-1.1)), floor(toInt32(-13), toFloat32(-1.1));
SELECT round(toInt32(-13), toFloat64(1.1)), ceil(toInt32(-13), toFloat64(-1.1)), floor(toInt32(-13), toFloat64(-1.1));

SELECT round(toInt32(-13), toInt8(-2)), ceil(toInt32(-13), toInt8(-2)), floor(toInt32(-13), toInt8(-2));
SELECT round(toInt32(-13), toInt16(-2)), ceil(toInt32(-13), toInt16(-2)), floor(toInt32(-13), toInt16(-2));
SELECT round(toInt32(-13), toInt32(-2)), ceil(toInt32(-13), toInt32(-2)), floor(toInt32(-13), toInt32(-2));
SELECT round(toInt32(-13), toInt64(-2)), ceil(toInt32(-13), toInt64(-2)), floor(toInt32(-13), toInt64(-2));
SELECT round(toInt32(-13), toFloat32(-2.1)), ceil(toInt32(-13), toFloat32(-2.1)), floor(toInt32(-13), toFloat32(-2.1));
SELECT round(toInt32(-13), toFloat64(-2.1)), ceil(toInt32(-13), toFloat64(-2.1)), floor(toInt32(-13), toFloat64(-2.1));

/* Int64 */

SELECT round(toInt64(-13), toUInt8(2)), ceil(toInt64(-13), toUInt8(2)), floor(toInt64(-13), toUInt8(2));
SELECT round(toInt64(-13), toUInt16(2)), ceil(toInt64(-13), toUInt16(2)), floor(toInt64(-13), toUInt16(2));
SELECT round(toInt64(-13), toUInt32(2)), ceil(toInt64(-13), toUInt32(2)), floor(toInt64(-13), toUInt32(2));
SELECT round(toInt64(-13), toUInt64(2)), ceil(toInt64(-13), toUInt64(2)), floor(toInt64(-13), toUInt64(2));
SELECT round(toInt64(-13), toInt8(2)), ceil(toInt64(-13), toInt8(2)), floor(toInt64(-13), toInt8(2));
SELECT round(toInt64(-13), toInt16(2)), ceil(toInt64(-13), toInt16(2)), floor(toInt64(-13), toInt16(2));
SELECT round(toInt64(-13), toInt32(2)), ceil(toInt64(-13), toInt32(2)), floor(toInt64(-13), toInt32(2));
SELECT round(toInt64(-13), toInt64(2)), ceil(toInt64(-13), toInt64(2)), floor(toInt64(-13), toInt64(2));
SELECT round(toInt64(-13), toFloat32(2.1)), ceil(toInt64(-13), toFloat32(2.1)), floor(toInt64(-13), toFloat32(2.1));
SELECT round(toInt64(-13), toFloat64(2.1)), ceil(toInt64(-13), toFloat64(2.1)), floor(toInt64(-13), toFloat64(2.1));

SELECT round(toInt64(-13), toUInt8(1)), ceil(toInt64(-13), toUInt8(1)), floor(toInt64(-13), toUInt8(1));
SELECT round(toInt64(-13), toUInt16(1)), ceil(toInt64(-13), toUInt16(1)), floor(toInt64(-13), toUInt16(1));
SELECT round(toInt64(-13), toUInt32(1)), ceil(toInt64(-13), toUInt32(1)), floor(toInt64(-13), toUInt32(1));
SELECT round(toInt64(-13), toUInt64(1)), ceil(toInt64(-13), toUInt64(1)), floor(toInt64(-13), toUInt64(1));
SELECT round(toInt64(-13), toInt8(1)), ceil(toInt64(-13), toInt8(1)), floor(toInt64(-13), toInt8(1));
SELECT round(toInt64(-13), toInt16(1)), ceil(toInt64(-13), toInt16(1)), floor(toInt64(-13), toInt16(1));
SELECT round(toInt64(-13), toInt32(1)), ceil(toInt64(-13), toInt32(1)), floor(toInt64(-13), toInt32(1));
SELECT round(toInt64(-13), toInt64(1)), ceil(toInt64(-13), toInt64(1)), floor(toInt64(-13), toInt64(1));
SELECT round(toInt64(-13), toFloat32(1.1)), ceil(toInt64(-13), toFloat32(1.1)), floor(toInt64(-13), toFloat32(1.1));
SELECT round(toInt64(-13), toFloat64(1.1)), ceil(toInt64(-13), toFloat64(1.1)), floor(toInt64(-13), toFloat64(1.1));

SELECT round(toInt64(-13), toUInt16(0)), ceil(toInt64(-13), toUInt16(0)), floor(toInt64(-13), toUInt16(0));
SELECT round(toInt64(-13), toUInt32(0)), ceil(toInt64(-13), toUInt32(0)), floor(toInt64(-13), toUInt32(0));
SELECT round(toInt64(-13), toUInt64(0)), ceil(toInt64(-13), toUInt64(0)), floor(toInt64(-13), toUInt64(0));
SELECT round(toInt64(-13), toInt8(0)), ceil(toInt64(-13), toInt8(0)), floor(toInt64(-13), toInt8(0));
SELECT round(toInt64(-13), toInt16(0)), ceil(toInt64(-13), toInt16(0)), floor(toInt64(-13), toInt16(0));
SELECT round(toInt64(-13), toInt32(0)), ceil(toInt64(-13), toInt32(0)), floor(toInt64(-13), toInt32(0));
SELECT round(toInt64(-13), toInt64(0)), ceil(toInt64(-13), toInt64(0)), floor(toInt64(-13), toInt64(0));
SELECT round(toInt64(-13), toFloat32(0.1)), ceil(toInt64(-13), toFloat32(0.1)), floor(toInt64(-13), toFloat32(0.1));
SELECT round(toInt64(-13), toFloat64(0.1)), ceil(toInt64(-13), toFloat64(0.1)), floor(toInt64(-13), toFloat64(0.1));

SELECT round(toInt64(-13), toInt8(-1)), ceil(toInt64(-13), toInt8(-1)), floor(toInt64(-13), toInt8(-1));
SELECT round(toInt64(-13), toInt16(-1)), ceil(toInt64(-13), toInt16(-1)), floor(toInt64(-13), toInt16(-1));
SELECT round(toInt64(-13), toInt32(-1)), ceil(toInt64(-13), toInt32(-1)), floor(toInt64(-13), toInt32(-1));
SELECT round(toInt64(-13), toInt64(-1)), ceil(toInt64(-13), toInt64(-1)), floor(toInt64(-13), toInt64(-1));
SELECT round(toInt64(-13), toFloat32(1.1)), ceil(toInt64(-13), toFloat32(-1.1)), floor(toInt64(-13), toFloat32(-1.1));
SELECT round(toInt64(-13), toFloat64(1.1)), ceil(toInt64(-13), toFloat64(-1.1)), floor(toInt64(-13), toFloat64(-1.1));

SELECT round(toInt64(-13), toInt8(-2)), ceil(toInt64(-13), toInt8(-2)), floor(toInt64(-13), toInt8(-2));
SELECT round(toInt64(-13), toInt16(-2)), ceil(toInt64(-13), toInt16(-2)), floor(toInt64(-13), toInt16(-2));
SELECT round(toInt64(-13), toInt32(-2)), ceil(toInt64(-13), toInt32(-2)), floor(toInt64(-13), toInt32(-2));
SELECT round(toInt64(-13), toInt64(-2)), ceil(toInt64(-13), toInt64(-2)), floor(toInt64(-13), toInt64(-2));
SELECT round(toInt64(-13), toFloat32(-2.1)), ceil(toInt64(-13), toFloat32(-2.1)), floor(toInt64(-13), toFloat32(-2.1));
SELECT round(toInt64(-13), toFloat64(-2.1)), ceil(toInt64(-13), toFloat64(-2.1)), floor(toInt64(-13), toFloat64(-2.1));

/* Float32 */

SELECT round(toFloat32(-13), toUInt8(2)), ceil(toFloat32(-13), toUInt8(2)), floor(toFloat32(-13), toUInt8(2));
SELECT round(toFloat32(-13), toUInt16(2)), ceil(toFloat32(-13), toUInt16(2)), floor(toFloat32(-13), toUInt16(2));
SELECT round(toFloat32(-13), toUInt32(2)), ceil(toFloat32(-13), toUInt32(2)), floor(toFloat32(-13), toUInt32(2));
SELECT round(toFloat32(-13), toUInt64(2)), ceil(toFloat32(-13), toUInt64(2)), floor(toFloat32(-13), toUInt64(2));
SELECT round(toFloat32(-13), toInt8(2)), ceil(toFloat32(-13), toInt8(2)), floor(toFloat32(-13), toInt8(2));
SELECT round(toFloat32(-13), toInt16(2)), ceil(toFloat32(-13), toInt16(2)), floor(toFloat32(-13), toInt16(2));
SELECT round(toFloat32(-13), toInt32(2)), ceil(toFloat32(-13), toInt32(2)), floor(toFloat32(-13), toInt32(2));
SELECT round(toFloat32(-13), toInt64(2)), ceil(toFloat32(-13), toInt64(2)), floor(toFloat32(-13), toInt64(2));
SELECT round(toFloat32(-13), toFloat32(2.1)), ceil(toFloat32(-13), toFloat32(2.1)), floor(toFloat32(-13), toFloat32(2.1));
SELECT round(toFloat32(-13), toFloat64(2.1)), ceil(toFloat32(-13), toFloat64(2.1)), floor(toFloat32(-13), toFloat64(2.1));

SELECT round(toFloat32(-13), toUInt8(1)), ceil(toFloat32(-13), toUInt8(1)), floor(toFloat32(-13), toUInt8(1));
SELECT round(toFloat32(-13), toUInt16(1)), ceil(toFloat32(-13), toUInt16(1)), floor(toFloat32(-13), toUInt16(1));
SELECT round(toFloat32(-13), toUInt32(1)), ceil(toFloat32(-13), toUInt32(1)), floor(toFloat32(-13), toUInt32(1));
SELECT round(toFloat32(-13), toUInt64(1)), ceil(toFloat32(-13), toUInt64(1)), floor(toFloat32(-13), toUInt64(1));
SELECT round(toFloat32(-13), toInt8(1)), ceil(toFloat32(-13), toInt8(1)), floor(toFloat32(-13), toInt8(1));
SELECT round(toFloat32(-13), toInt16(1)), ceil(toFloat32(-13), toInt16(1)), floor(toFloat32(-13), toInt16(1));
SELECT round(toFloat32(-13), toInt32(1)), ceil(toFloat32(-13), toInt32(1)), floor(toFloat32(-13), toInt32(1));
SELECT round(toFloat32(-13), toInt64(1)), ceil(toFloat32(-13), toInt64(1)), floor(toFloat32(-13), toInt64(1));
SELECT round(toFloat32(-13), toFloat32(1.1)), ceil(toFloat32(-13), toFloat32(1.1)), floor(toFloat32(-13), toFloat32(1.1));
SELECT round(toFloat32(-13), toFloat64(1.1)), ceil(toFloat32(-13), toFloat64(1.1)), floor(toFloat32(-13), toFloat64(1.1));

SELECT round(toFloat32(-13), toUInt16(0)), ceil(toFloat32(-13), toUInt16(0)), floor(toFloat32(-13), toUInt16(0));
SELECT round(toFloat32(-13), toUInt32(0)), ceil(toFloat32(-13), toUInt32(0)), floor(toFloat32(-13), toUInt32(0));
SELECT round(toFloat32(-13), toUInt64(0)), ceil(toFloat32(-13), toUInt64(0)), floor(toFloat32(-13), toUInt64(0));
SELECT round(toFloat32(-13), toInt8(0)), ceil(toFloat32(-13), toInt8(0)), floor(toFloat32(-13), toInt8(0));
SELECT round(toFloat32(-13), toInt16(0)), ceil(toFloat32(-13), toInt16(0)), floor(toFloat32(-13), toInt16(0));
SELECT round(toFloat32(-13), toInt32(0)), ceil(toFloat32(-13), toInt32(0)), floor(toFloat32(-13), toInt32(0));
SELECT round(toFloat32(-13), toInt64(0)), ceil(toFloat32(-13), toInt64(0)), floor(toFloat32(-13), toInt64(0));
SELECT round(toFloat32(-13), toFloat32(0.1)), ceil(toFloat32(-13), toFloat32(0.1)), floor(toFloat32(-13), toFloat32(0.1));
SELECT round(toFloat32(-13), toFloat64(0.1)), ceil(toFloat32(-13), toFloat64(0.1)), floor(toFloat32(-13), toFloat64(0.1));

SELECT round(toFloat32(-13), toInt8(-1)), ceil(toFloat32(-13), toInt8(-1)), floor(toFloat32(-13), toInt8(-1));
SELECT round(toFloat32(-13), toInt16(-1)), ceil(toFloat32(-13), toInt16(-1)), floor(toFloat32(-13), toInt16(-1));
SELECT round(toFloat32(-13), toInt32(-1)), ceil(toFloat32(-13), toInt32(-1)), floor(toFloat32(-13), toInt32(-1));
SELECT round(toFloat32(-13), toInt64(-1)), ceil(toFloat32(-13), toInt64(-1)), floor(toFloat32(-13), toInt64(-1));
SELECT round(toFloat32(-13), toFloat32(1.1)), ceil(toFloat32(-13), toFloat32(-1.1)), floor(toFloat32(-13), toFloat32(-1.1));
SELECT round(toFloat32(-13), toFloat64(1.1)), ceil(toFloat32(-13), toFloat64(-1.1)), floor(toFloat32(-13), toFloat64(-1.1));

SELECT round(toFloat32(-13), toInt8(-2)), ceil(toFloat32(-13), toInt8(-2)), floor(toFloat32(-13), toInt8(-2));
SELECT round(toFloat32(-13), toInt16(-2)), ceil(toFloat32(-13), toInt16(-2)), floor(toFloat32(-13), toInt16(-2));
SELECT round(toFloat32(-13), toInt32(-2)), ceil(toFloat32(-13), toInt32(-2)), floor(toFloat32(-13), toInt32(-2));
SELECT round(toFloat32(-13), toInt64(-2)), ceil(toFloat32(-13), toInt64(-2)), floor(toFloat32(-13), toInt64(-2));
SELECT round(toFloat32(-13), toFloat32(-2.1)), ceil(toFloat32(-13), toFloat32(-2.1)), floor(toFloat32(-13), toFloat32(-2.1));
SELECT round(toFloat32(-13), toFloat64(-2.1)), ceil(toFloat32(-13), toFloat64(-2.1)), floor(toFloat32(-13), toFloat64(-2.1));

/* Float64 */

SELECT round(toFloat64(-13), toUInt8(2)), ceil(toFloat64(-13), toUInt8(2)), floor(toFloat64(-13), toUInt8(2));
SELECT round(toFloat64(-13), toUInt16(2)), ceil(toFloat64(-13), toUInt16(2)), floor(toFloat64(-13), toUInt16(2));
SELECT round(toFloat64(-13), toUInt32(2)), ceil(toFloat64(-13), toUInt32(2)), floor(toFloat64(-13), toUInt32(2));
SELECT round(toFloat64(-13), toUInt64(2)), ceil(toFloat64(-13), toUInt64(2)), floor(toFloat64(-13), toUInt64(2));
SELECT round(toFloat64(-13), toInt8(2)), ceil(toFloat64(-13), toInt8(2)), floor(toFloat64(-13), toInt8(2));
SELECT round(toFloat64(-13), toInt16(2)), ceil(toFloat64(-13), toInt16(2)), floor(toFloat64(-13), toInt16(2));
SELECT round(toFloat64(-13), toInt32(2)), ceil(toFloat64(-13), toInt32(2)), floor(toFloat64(-13), toInt32(2));
SELECT round(toFloat64(-13), toInt64(2)), ceil(toFloat64(-13), toInt64(2)), floor(toFloat64(-13), toInt64(2));
SELECT round(toFloat64(-13), toFloat32(2.1)), ceil(toFloat64(-13), toFloat32(2.1)), floor(toFloat64(-13), toFloat32(2.1));
SELECT round(toFloat64(-13), toFloat64(2.1)), ceil(toFloat64(-13), toFloat64(2.1)), floor(toFloat64(-13), toFloat64(2.1));

SELECT round(toFloat64(-13), toUInt8(1)), ceil(toFloat64(-13), toUInt8(1)), floor(toFloat64(-13), toUInt8(1));
SELECT round(toFloat64(-13), toUInt16(1)), ceil(toFloat64(-13), toUInt16(1)), floor(toFloat64(-13), toUInt16(1));
SELECT round(toFloat64(-13), toUInt32(1)), ceil(toFloat64(-13), toUInt32(1)), floor(toFloat64(-13), toUInt32(1));
SELECT round(toFloat64(-13), toUInt64(1)), ceil(toFloat64(-13), toUInt64(1)), floor(toFloat64(-13), toUInt64(1));
SELECT round(toFloat64(-13), toInt8(1)), ceil(toFloat64(-13), toInt8(1)), floor(toFloat64(-13), toInt8(1));
SELECT round(toFloat64(-13), toInt16(1)), ceil(toFloat64(-13), toInt16(1)), floor(toFloat64(-13), toInt16(1));
SELECT round(toFloat64(-13), toInt32(1)), ceil(toFloat64(-13), toInt32(1)), floor(toFloat64(-13), toInt32(1));
SELECT round(toFloat64(-13), toInt64(1)), ceil(toFloat64(-13), toInt64(1)), floor(toFloat64(-13), toInt64(1));
SELECT round(toFloat64(-13), toFloat32(1.1)), ceil(toFloat64(-13), toFloat32(1.1)), floor(toFloat64(-13), toFloat32(1.1));
SELECT round(toFloat64(-13), toFloat64(1.1)), ceil(toFloat64(-13), toFloat64(1.1)), floor(toFloat64(-13), toFloat64(1.1));

SELECT round(toFloat64(-13), toUInt16(0)), ceil(toFloat64(-13), toUInt16(0)), floor(toFloat64(-13), toUInt16(0));
SELECT round(toFloat64(-13), toUInt32(0)), ceil(toFloat64(-13), toUInt32(0)), floor(toFloat64(-13), toUInt32(0));
SELECT round(toFloat64(-13), toUInt64(0)), ceil(toFloat64(-13), toUInt64(0)), floor(toFloat64(-13), toUInt64(0));
SELECT round(toFloat64(-13), toInt8(0)), ceil(toFloat64(-13), toInt8(0)), floor(toFloat64(-13), toInt8(0));
SELECT round(toFloat64(-13), toInt16(0)), ceil(toFloat64(-13), toInt16(0)), floor(toFloat64(-13), toInt16(0));
SELECT round(toFloat64(-13), toInt32(0)), ceil(toFloat64(-13), toInt32(0)), floor(toFloat64(-13), toInt32(0));
SELECT round(toFloat64(-13), toInt64(0)), ceil(toFloat64(-13), toInt64(0)), floor(toFloat64(-13), toInt64(0));
SELECT round(toFloat64(-13), toFloat32(0.1)), ceil(toFloat64(-13), toFloat32(0.1)), floor(toFloat64(-13), toFloat32(0.1));
SELECT round(toFloat64(-13), toFloat64(0.1)), ceil(toFloat64(-13), toFloat64(0.1)), floor(toFloat64(-13), toFloat64(0.1));

SELECT round(toFloat64(-13), toInt8(-1)), ceil(toFloat64(-13), toInt8(-1)), floor(toFloat64(-13), toInt8(-1));
SELECT round(toFloat64(-13), toInt16(-1)), ceil(toFloat64(-13), toInt16(-1)), floor(toFloat64(-13), toInt16(-1));
SELECT round(toFloat64(-13), toInt32(-1)), ceil(toFloat64(-13), toInt32(-1)), floor(toFloat64(-13), toInt32(-1));
SELECT round(toFloat64(-13), toInt64(-1)), ceil(toFloat64(-13), toInt64(-1)), floor(toFloat64(-13), toInt64(-1));
SELECT round(toFloat64(-13), toFloat32(1.1)), ceil(toFloat64(-13), toFloat32(-1.1)), floor(toFloat64(-13), toFloat32(-1.1));
SELECT round(toFloat64(-13), toFloat64(1.1)), ceil(toFloat64(-13), toFloat64(-1.1)), floor(toFloat64(-13), toFloat64(-1.1));

SELECT round(toFloat64(-13), toInt8(-2)), ceil(toFloat64(-13), toInt8(-2)), floor(toFloat64(-13), toInt8(-2));
SELECT round(toFloat64(-13), toInt16(-2)), ceil(toFloat64(-13), toInt16(-2)), floor(toFloat64(-13), toInt16(-2));
SELECT round(toFloat64(-13), toInt32(-2)), ceil(toFloat64(-13), toInt32(-2)), floor(toFloat64(-13), toInt32(-2));
SELECT round(toFloat64(-13), toInt64(-2)), ceil(toFloat64(-13), toInt64(-2)), floor(toFloat64(-13), toInt64(-2));
SELECT round(toFloat64(-13), toFloat32(-2.1)), ceil(toFloat64(-13), toFloat32(-2.1)), floor(toFloat64(-13), toFloat32(-2.1));
SELECT round(toFloat64(-13), toFloat64(-2.1)), ceil(toFloat64(-13), toFloat64(-2.1)), floor(toFloat64(-13), toFloat64(-2.1));

/* Положительное число с плавающей точкой */

SELECT round(toFloat64(2.718281828459), toUInt8(2)), ceil(toFloat64(2.718281828459), toUInt8(2)), floor(toFloat64(2.718281828459), toUInt8(2));
SELECT round(toFloat64(2.718281828459), toUInt16(2)), ceil(toFloat64(2.718281828459), toUInt16(2)), floor(toFloat64(2.718281828459), toUInt16(2));
SELECT round(toFloat64(2.718281828459), toUInt32(2)), ceil(toFloat64(2.718281828459), toUInt32(2)), floor(toFloat64(2.718281828459), toUInt32(2));
SELECT round(toFloat64(2.718281828459), toUInt64(2)), ceil(toFloat64(2.718281828459), toUInt64(2)), floor(toFloat64(2.718281828459), toUInt64(2));
SELECT round(toFloat64(2.718281828459), toInt8(2)), ceil(toFloat64(2.718281828459), toInt8(2)), floor(toFloat64(2.718281828459), toInt8(2));
SELECT round(toFloat64(2.718281828459), toInt16(2)), ceil(toFloat64(2.718281828459), toInt16(2)), floor(toFloat64(2.718281828459), toInt16(2));
SELECT round(toFloat64(2.718281828459), toInt32(2)), ceil(toFloat64(2.718281828459), toInt32(2)), floor(toFloat64(2.718281828459), toInt32(2));
SELECT round(toFloat64(2.718281828459), toInt64(2)), ceil(toFloat64(2.718281828459), toInt64(2)), floor(toFloat64(2.718281828459), toInt64(2));
SELECT round(toFloat64(2.718281828459), toFloat32(2.1)), ceil(toFloat64(2.718281828459), toFloat32(2.1)), floor(toFloat64(2.718281828459), toFloat32(2.1));
SELECT round(toFloat64(2.718281828459), toFloat64(2.1)), ceil(toFloat64(2.718281828459), toFloat64(2.1)), floor(toFloat64(2.718281828459), toFloat64(2.1));

SELECT round(toFloat64(2.718281828459), toUInt8(1)), ceil(toFloat64(2.718281828459), toUInt8(1)), floor(toFloat64(2.718281828459), toUInt8(1));
SELECT round(toFloat64(2.718281828459), toUInt16(1)), ceil(toFloat64(2.718281828459), toUInt16(1)), floor(toFloat64(2.718281828459), toUInt16(1));
SELECT round(toFloat64(2.718281828459), toUInt32(1)), ceil(toFloat64(2.718281828459), toUInt32(1)), floor(toFloat64(2.718281828459), toUInt32(1));
SELECT round(toFloat64(2.718281828459), toUInt64(1)), ceil(toFloat64(2.718281828459), toUInt64(1)), floor(toFloat64(2.718281828459), toUInt64(1));
SELECT round(toFloat64(2.718281828459), toInt8(1)), ceil(toFloat64(2.718281828459), toInt8(1)), floor(toFloat64(2.718281828459), toInt8(1));
SELECT round(toFloat64(2.718281828459), toInt16(1)), ceil(toFloat64(2.718281828459), toInt16(1)), floor(toFloat64(2.718281828459), toInt16(1));
SELECT round(toFloat64(2.718281828459), toInt32(1)), ceil(toFloat64(2.718281828459), toInt32(1)), floor(toFloat64(2.718281828459), toInt32(1));
SELECT round(toFloat64(2.718281828459), toInt64(1)), ceil(toFloat64(2.718281828459), toInt64(1)), floor(toFloat64(2.718281828459), toInt64(1));
SELECT round(toFloat64(2.718281828459), toFloat32(1.1)), ceil(toFloat64(2.718281828459), toFloat32(1.1)), floor(toFloat64(2.718281828459), toFloat32(1.1));
SELECT round(toFloat64(2.718281828459), toFloat64(1.1)), ceil(toFloat64(2.718281828459), toFloat64(1.1)), floor(toFloat64(2.718281828459), toFloat64(1.1));

SELECT round(toFloat64(2.718281828459), toUInt16(0)), ceil(toFloat64(2.718281828459), toUInt16(0)), floor(toFloat64(2.718281828459), toUInt16(0));
SELECT round(toFloat64(2.718281828459), toUInt32(0)), ceil(toFloat64(2.718281828459), toUInt32(0)), floor(toFloat64(2.718281828459), toUInt32(0));
SELECT round(toFloat64(2.718281828459), toUInt64(0)), ceil(toFloat64(2.718281828459), toUInt64(0)), floor(toFloat64(2.718281828459), toUInt64(0));
SELECT round(toFloat64(2.718281828459), toInt8(0)), ceil(toFloat64(2.718281828459), toInt8(0)), floor(toFloat64(2.718281828459), toInt8(0));
SELECT round(toFloat64(2.718281828459), toInt16(0)), ceil(toFloat64(2.718281828459), toInt16(0)), floor(toFloat64(2.718281828459), toInt16(0));
SELECT round(toFloat64(2.718281828459), toInt32(0)), ceil(toFloat64(2.718281828459), toInt32(0)), floor(toFloat64(2.718281828459), toInt32(0));
SELECT round(toFloat64(2.718281828459), toInt64(0)), ceil(toFloat64(2.718281828459), toInt64(0)), floor(toFloat64(2.718281828459), toInt64(0));
SELECT round(toFloat64(2.718281828459), toFloat32(0.1)), ceil(toFloat64(2.718281828459), toFloat32(0.1)), floor(toFloat64(2.718281828459), toFloat32(0.1));
SELECT round(toFloat64(2.718281828459), toFloat64(0.1)), ceil(toFloat64(2.718281828459), toFloat64(0.1)), floor(toFloat64(2.718281828459), toFloat64(0.1));

SELECT round(toFloat64(2.718281828459), toInt8(-1)), ceil(toFloat64(2.718281828459), toInt8(-1)), floor(toFloat64(2.718281828459), toInt8(-1));
SELECT round(toFloat64(2.718281828459), toInt16(-1)), ceil(toFloat64(2.718281828459), toInt16(-1)), floor(toFloat64(2.718281828459), toInt16(-1));
SELECT round(toFloat64(2.718281828459), toInt32(-1)), ceil(toFloat64(2.718281828459), toInt32(-1)), floor(toFloat64(2.718281828459), toInt32(-1));
SELECT round(toFloat64(2.718281828459), toInt64(-1)), ceil(toFloat64(2.718281828459), toInt64(-1)), floor(toFloat64(2.718281828459), toInt64(-1));
SELECT round(toFloat64(2.718281828459), toFloat32(1.1)), ceil(toFloat64(2.718281828459), toFloat32(-1.1)), floor(toFloat64(2.718281828459), toFloat32(-1.1));
SELECT round(toFloat64(2.718281828459), toFloat64(1.1)), ceil(toFloat64(2.718281828459), toFloat64(-1.1)), floor(toFloat64(2.718281828459), toFloat64(-1.1));

SELECT round(toFloat64(2.718281828459), toInt8(-2)), ceil(toFloat64(2.718281828459), toInt8(-2)), floor(toFloat64(2.718281828459), toInt8(-2));
SELECT round(toFloat64(2.718281828459), toInt16(-2)), ceil(toFloat64(2.718281828459), toInt16(-2)), floor(toFloat64(2.718281828459), toInt16(-2));
SELECT round(toFloat64(2.718281828459), toInt32(-2)), ceil(toFloat64(2.718281828459), toInt32(-2)), floor(toFloat64(2.718281828459), toInt32(-2));
SELECT round(toFloat64(2.718281828459), toInt64(-2)), ceil(toFloat64(2.718281828459), toInt64(-2)), floor(toFloat64(2.718281828459), toInt64(-2));
SELECT round(toFloat64(2.718281828459), toFloat32(-2.1)), ceil(toFloat64(2.718281828459), toFloat32(-2.1)), floor(toFloat64(2.718281828459), toFloat32(-2.1));
SELECT round(toFloat64(2.718281828459), toFloat64(-2.1)), ceil(toFloat64(2.718281828459), toFloat64(-2.1)), floor(toFloat64(2.718281828459), toFloat64(-2.1));

/* Отрицательное число с плавающей точкой */

SELECT round(toFloat64(-2.718281828459), toUInt8(2)), ceil(toFloat64(-2.718281828459), toUInt8(2)), floor(toFloat64(-2.718281828459), toUInt8(2));
SELECT round(toFloat64(-2.718281828459), toUInt16(2)), ceil(toFloat64(-2.718281828459), toUInt16(2)), floor(toFloat64(-2.718281828459), toUInt16(2));
SELECT round(toFloat64(-2.718281828459), toUInt32(2)), ceil(toFloat64(-2.718281828459), toUInt32(2)), floor(toFloat64(-2.718281828459), toUInt32(2));
SELECT round(toFloat64(-2.718281828459), toUInt64(2)), ceil(toFloat64(-2.718281828459), toUInt64(2)), floor(toFloat64(-2.718281828459), toUInt64(2));
SELECT round(toFloat64(-2.718281828459), toInt8(2)), ceil(toFloat64(-2.718281828459), toInt8(2)), floor(toFloat64(-2.718281828459), toInt8(2));
SELECT round(toFloat64(-2.718281828459), toInt16(2)), ceil(toFloat64(-2.718281828459), toInt16(2)), floor(toFloat64(-2.718281828459), toInt16(2));
SELECT round(toFloat64(-2.718281828459), toInt32(2)), ceil(toFloat64(-2.718281828459), toInt32(2)), floor(toFloat64(-2.718281828459), toInt32(2));
SELECT round(toFloat64(-2.718281828459), toInt64(2)), ceil(toFloat64(-2.718281828459), toInt64(2)), floor(toFloat64(-2.718281828459), toInt64(2));
SELECT round(toFloat64(-2.718281828459), toFloat32(2.1)), ceil(toFloat64(-2.718281828459), toFloat32(2.1)), floor(toFloat64(-2.718281828459), toFloat32(2.1));
SELECT round(toFloat64(-2.718281828459), toFloat64(2.1)), ceil(toFloat64(-2.718281828459), toFloat64(2.1)), floor(toFloat64(-2.718281828459), toFloat64(2.1));

SELECT round(toFloat64(-2.718281828459), toUInt8(1)), ceil(toFloat64(-2.718281828459), toUInt8(1)), floor(toFloat64(-2.718281828459), toUInt8(1));
SELECT round(toFloat64(-2.718281828459), toUInt16(1)), ceil(toFloat64(-2.718281828459), toUInt16(1)), floor(toFloat64(-2.718281828459), toUInt16(1));
SELECT round(toFloat64(-2.718281828459), toUInt32(1)), ceil(toFloat64(-2.718281828459), toUInt32(1)), floor(toFloat64(-2.718281828459), toUInt32(1));
SELECT round(toFloat64(-2.718281828459), toUInt64(1)), ceil(toFloat64(-2.718281828459), toUInt64(1)), floor(toFloat64(-2.718281828459), toUInt64(1));
SELECT round(toFloat64(-2.718281828459), toInt8(1)), ceil(toFloat64(-2.718281828459), toInt8(1)), floor(toFloat64(-2.718281828459), toInt8(1));
SELECT round(toFloat64(-2.718281828459), toInt16(1)), ceil(toFloat64(-2.718281828459), toInt16(1)), floor(toFloat64(-2.718281828459), toInt16(1));
SELECT round(toFloat64(-2.718281828459), toInt32(1)), ceil(toFloat64(-2.718281828459), toInt32(1)), floor(toFloat64(-2.718281828459), toInt32(1));
SELECT round(toFloat64(-2.718281828459), toInt64(1)), ceil(toFloat64(-2.718281828459), toInt64(1)), floor(toFloat64(-2.718281828459), toInt64(1));
SELECT round(toFloat64(-2.718281828459), toFloat32(1.1)), ceil(toFloat64(-2.718281828459), toFloat32(1.1)), floor(toFloat64(-2.718281828459), toFloat32(1.1));
SELECT round(toFloat64(-2.718281828459), toFloat64(1.1)), ceil(toFloat64(-2.718281828459), toFloat64(1.1)), floor(toFloat64(-2.718281828459), toFloat64(1.1));

SELECT round(toFloat64(-2.718281828459), toUInt16(0)), ceil(toFloat64(-2.718281828459), toUInt16(0)), floor(toFloat64(-2.718281828459), toUInt16(0));
SELECT round(toFloat64(-2.718281828459), toUInt32(0)), ceil(toFloat64(-2.718281828459), toUInt32(0)), floor(toFloat64(-2.718281828459), toUInt32(0));
SELECT round(toFloat64(-2.718281828459), toUInt64(0)), ceil(toFloat64(-2.718281828459), toUInt64(0)), floor(toFloat64(-2.718281828459), toUInt64(0));
SELECT round(toFloat64(-2.718281828459), toInt8(0)), ceil(toFloat64(-2.718281828459), toInt8(0)), floor(toFloat64(-2.718281828459), toInt8(0));
SELECT round(toFloat64(-2.718281828459), toInt16(0)), ceil(toFloat64(-2.718281828459), toInt16(0)), floor(toFloat64(-2.718281828459), toInt16(0));
SELECT round(toFloat64(-2.718281828459), toInt32(0)), ceil(toFloat64(-2.718281828459), toInt32(0)), floor(toFloat64(-2.718281828459), toInt32(0));
SELECT round(toFloat64(-2.718281828459), toInt64(0)), ceil(toFloat64(-2.718281828459), toInt64(0)), floor(toFloat64(-2.718281828459), toInt64(0));
SELECT round(toFloat64(-2.718281828459), toFloat32(0.1)), ceil(toFloat64(-2.718281828459), toFloat32(0.1)), floor(toFloat64(-2.718281828459), toFloat32(0.1));
SELECT round(toFloat64(-2.718281828459), toFloat64(0.1)), ceil(toFloat64(-2.718281828459), toFloat64(0.1)), floor(toFloat64(-2.718281828459), toFloat64(0.1));

SELECT round(toFloat64(-2.718281828459), toInt8(-1)), ceil(toFloat64(-2.718281828459), toInt8(-1)), floor(toFloat64(-2.718281828459), toInt8(-1));
SELECT round(toFloat64(-2.718281828459), toInt16(-1)), ceil(toFloat64(-2.718281828459), toInt16(-1)), floor(toFloat64(-2.718281828459), toInt16(-1));
SELECT round(toFloat64(-2.718281828459), toInt32(-1)), ceil(toFloat64(-2.718281828459), toInt32(-1)), floor(toFloat64(-2.718281828459), toInt32(-1));
SELECT round(toFloat64(-2.718281828459), toInt64(-1)), ceil(toFloat64(-2.718281828459), toInt64(-1)), floor(toFloat64(-2.718281828459), toInt64(-1));
SELECT round(toFloat64(-2.718281828459), toFloat32(1.1)), ceil(toFloat64(-2.718281828459), toFloat32(-1.1)), floor(toFloat64(-2.718281828459), toFloat32(-1.1));
SELECT round(toFloat64(-2.718281828459), toFloat64(1.1)), ceil(toFloat64(-2.718281828459), toFloat64(-1.1)), floor(toFloat64(-2.718281828459), toFloat64(-1.1));

SELECT round(toFloat64(-2.718281828459), toInt8(-2)), ceil(toFloat64(-2.718281828459), toInt8(-2)), floor(toFloat64(-2.718281828459), toInt8(-2));
SELECT round(toFloat64(-2.718281828459), toInt16(-2)), ceil(toFloat64(-2.718281828459), toInt16(-2)), floor(toFloat64(-2.718281828459), toInt16(-2));
SELECT round(toFloat64(-2.718281828459), toInt32(-2)), ceil(toFloat64(-2.718281828459), toInt32(-2)), floor(toFloat64(-2.718281828459), toInt32(-2));
SELECT round(toFloat64(-2.718281828459), toInt64(-2)), ceil(toFloat64(-2.718281828459), toInt64(-2)), floor(toFloat64(-2.718281828459), toInt64(-2));
SELECT round(toFloat64(-2.718281828459), toFloat32(-2.1)), ceil(toFloat64(-2.718281828459), toFloat32(-2.1)), floor(toFloat64(-2.718281828459), toFloat32(-2.1));
SELECT round(toFloat64(-2.718281828459), toFloat64(-2.1)), ceil(toFloat64(-2.718281828459), toFloat64(-2.1)), floor(toFloat64(-2.718281828459), toFloat64(-2.1));

/* Misc. */

SELECT round(13112221, -1), ceil(13112221, -1), floor(13112221, -1);
SELECT round(13112221, -2), ceil(13112221, -2), floor(13112221, -2);
SELECT round(13112221, -3), ceil(13112221, -3), floor(13112221, -3);
SELECT round(13112221, -4), ceil(13112221, -4), floor(13112221, -4);
SELECT round(13112221, -5), ceil(13112221, -5), floor(13112221, -5);
SELECT round(13112221, -6), ceil(13112221, -6), floor(13112221, -6);
SELECT round(13112221, -7), ceil(13112221, -7), floor(13112221, -7);
SELECT round(13112221, -8), ceil(13112221, -8), floor(13112221, -8);
SELECT round(13112221, -9), ceil(13112221, -9), floor(13112221, -9);
SELECT round(13112221, -10), ceil(13112221, -10), floor(13112221, -10);
SELECT round(13112221, -11), ceil(13112221, -11), floor(13112221, -11);
SELECT round(13112221, -12), ceil(13112221, -12), floor(13112221, -12);
SELECT round(13112221, -13), ceil(13112221, -13), floor(13112221, -13);
SELECT round(13112221, -14), ceil(13112221, -14), floor(13112221, -14);
SELECT round(13112221, -15), ceil(13112221, -15), floor(13112221, -15);
SELECT round(13112221, -16), ceil(13112221, -16), floor(13112221, -16);
SELECT round(13112221, -17), ceil(13112221, -17), floor(13112221, -17);
SELECT round(13112221, -18), ceil(13112221, -18), floor(13112221, -18);
SELECT round(13112221, -19), ceil(13112221, -19), floor(13112221, -19);
SELECT round(13112221, -20), ceil(13112221, -20), floor(13112221, -20);

SELECT round(2.718281828459045, 1), ceil(2.718281828459045, 1), floor(2.718281828459045, 1);
SELECT round(2.718281828459045, 2), ceil(2.718281828459045, 2), floor(2.718281828459045, 2);
SELECT round(2.718281828459045, 3), ceil(2.718281828459045, 3), floor(2.718281828459045, 3);
SELECT round(2.718281828459045, 4), ceil(2.718281828459045, 4), floor(2.718281828459045, 4);
SELECT round(2.718281828459045, 5), ceil(2.718281828459045, 5), floor(2.718281828459045, 5);
SELECT round(2.718281828459045, 6), ceil(2.718281828459045, 6), floor(2.718281828459045, 6);
SELECT round(2.718281828459045, 7), ceil(2.718281828459045, 7), floor(2.718281828459045, 7);
SELECT round(2.718281828459045, 8), ceil(2.718281828459045, 8), floor(2.718281828459045, 8);
SELECT round(2.718281828459045, 9), ceil(2.718281828459045, 9), floor(2.718281828459045, 9);
SELECT round(2.718281828459045, 10), ceil(2.718281828459045, 10), floor(2.718281828459045, 10);
SELECT round(2.718281828459045, 11), ceil(2.718281828459045, 11), floor(2.718281828459045, 11);
SELECT round(2.718281828459045, 12), ceil(2.718281828459045, 12), floor(2.718281828459045, 12);
SELECT round(2.718281828459045, 13), ceil(2.718281828459045, 13), floor(2.718281828459045, 13);
SELECT round(2.718281828459045, 14), ceil(2.718281828459045, 14), floor(2.718281828459045, 14);
SELECT round(2.718281828459045, 15), ceil(2.718281828459045, 15), floor(2.718281828459045, 15);
SELECT round(2.718281828459045, 16), ceil(2.718281828459045, 16), floor(2.718281828459045, 16);
SELECT round(2.718281828459045, 17), ceil(2.718281828459045, 17), floor(2.718281828459045, 17);
SELECT round(2.718281828459045, 18), ceil(2.718281828459045, 18), floor(2.718281828459045, 18);
SELECT round(2.718281828459045, 19), ceil(2.718281828459045, 19), floor(2.718281828459045, 19);
SELECT round(2.718281828459045, 20), ceil(2.718281828459045, 20), floor(2.718281828459045, 20);

SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(1) AS x);
SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(2) AS x);
SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(3) AS x);
SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(4) AS x);
SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(5) AS x);
SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(6) AS x);
SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(7) AS x);
SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(8) AS x);
SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(9) AS x);
SELECT round(y,3) FROM (SELECT 2.718281828459045 + 1/(1+x*x) AS y FROM system.one ARRAY JOIN range(10) AS x);

/* Negative zeroes. */

SELECT round(-0.002);
SELECT round(-0.002, -1);
SELECT round(-0.002, 1);
