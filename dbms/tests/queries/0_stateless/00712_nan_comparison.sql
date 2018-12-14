SELECT nan = toUInt8(0), nan != toUInt8(0), nan < toUInt8(0), nan > toUInt8(0), nan <= toUInt8(0), nan >= toUInt8(0);
SELECT nan = toInt8(0), nan != toInt8(0), nan < toInt8(0), nan > toInt8(0), nan <= toInt8(0), nan >= toInt8(0);
SELECT nan = toUInt16(0), nan != toUInt16(0), nan < toUInt16(0), nan > toUInt16(0), nan <= toUInt16(0), nan >= toUInt16(0);
SELECT nan = toInt16(0), nan != toInt16(0), nan < toInt16(0), nan > toInt16(0), nan <= toInt16(0), nan >= toInt16(0);
SELECT nan = toUInt32(0), nan != toUInt32(0), nan < toUInt32(0), nan > toUInt32(0), nan <= toUInt32(0), nan >= toUInt32(0);
SELECT nan = toInt32(0), nan != toInt32(0), nan < toInt32(0), nan > toInt32(0), nan <= toInt32(0), nan >= toInt32(0);
SELECT nan = toUInt64(0), nan != toUInt64(0), nan < toUInt64(0), nan > toUInt64(0), nan <= toUInt64(0), nan >= toUInt64(0);
SELECT nan = toInt64(0), nan != toInt64(0), nan < toInt64(0), nan > toInt64(0), nan <= toInt64(0), nan >= toInt64(0);
SELECT nan = toFloat32(0.0), nan != toFloat32(0.0), nan < toFloat32(0.0), nan > toFloat32(0.0), nan <= toFloat32(0.0), nan >= toFloat32(0.0);
SELECT nan = toFloat64(0.0), nan != toFloat64(0.0), nan < toFloat64(0.0), nan > toFloat64(0.0), nan <= toFloat64(0.0), nan >= toFloat64(0.0);

SELECT -nan = toUInt8(0), -nan != toUInt8(0), -nan < toUInt8(0), -nan > toUInt8(0), -nan <= toUInt8(0), -nan >= toUInt8(0);
SELECT -nan = toInt8(0), -nan != toInt8(0), -nan < toInt8(0), -nan > toInt8(0), -nan <= toInt8(0), -nan >= toInt8(0);
SELECT -nan = toUInt16(0), -nan != toUInt16(0), -nan < toUInt16(0), -nan > toUInt16(0), -nan <= toUInt16(0), -nan >= toUInt16(0);
SELECT -nan = toInt16(0), -nan != toInt16(0), -nan < toInt16(0), -nan > toInt16(0), -nan <= toInt16(0), -nan >= toInt16(0);
SELECT -nan = toUInt32(0), -nan != toUInt32(0), -nan < toUInt32(0), -nan > toUInt32(0), -nan <= toUInt32(0), -nan >= toUInt32(0);
SELECT -nan = toInt32(0), -nan != toInt32(0), -nan < toInt32(0), -nan > toInt32(0), -nan <= toInt32(0), -nan >= toInt32(0);
SELECT -nan = toUInt64(0), -nan != toUInt64(0), -nan < toUInt64(0), -nan > toUInt64(0), -nan <= toUInt64(0), -nan >= toUInt64(0);
SELECT -nan = toInt64(0), -nan != toInt64(0), -nan < toInt64(0), -nan > toInt64(0), -nan <= toInt64(0), -nan >= toInt64(0);
SELECT -nan = toFloat32(0.0), -nan != toFloat32(0.0), -nan < toFloat32(0.0), -nan > toFloat32(0.0), -nan <= toFloat32(0.0), -nan >= toFloat32(0.0);
SELECT -nan = toFloat64(0.0), -nan != toFloat64(0.0), -nan < toFloat64(0.0), -nan > toFloat64(0.0), -nan <= toFloat64(0.0), -nan >= toFloat64(0.0);

SELECT nan = nan, nan != nan, nan = -nan, nan != -nan;
SELECT nan < nan, nan <= nan, nan < -nan, nan <= -nan;
SELECT nan > nan, nan >= nan, nan > -nan, nan >= -nan;
SELECT -nan < -nan, -nan <= -nan, -nan < nan, -nan <= nan;
SELECT -nan > -nan, -nan >= -nan, -nan > nan, -nan >= nan;

--SELECT 1 % nan, nan % 1, pow(x, 1), pow(1, x); -- TODO
SELECT 1 + nan, 1 - nan, nan - 1, 1 * nan, 1 / nan, nan / 1;
SELECT nan AS x, exp(x), exp2(x), exp10(x), log(x), log2(x), log10(x), sqrt(x), cbrt(x);
SELECT nan AS x, erf(x), erfc(x), lgamma(x), tgamma(x);
SELECT nan AS x, sin(x), cos(x), tan(x), asin(x), acos(x), atan(x);

SELECT min(x), max(x) FROM (SELECT arrayJoin([toFloat32(0.0), nan, toFloat32(1.0), toFloat32(-1.0)]) AS x);
SELECT min(x), max(x) FROM (SELECT arrayJoin([toFloat64(0.0), -nan, toFloat64(1.0), toFloat64(-1.0)]) AS x);
