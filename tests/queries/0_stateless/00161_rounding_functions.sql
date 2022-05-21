SELECT toUInt8(number) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toUInt16(number) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toUInt32(number) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toUInt64(number) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toInt8(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toInt16(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toInt32(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toInt64(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toFloat32(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toFloat64(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;

SELECT toFloat32((number - 10) / 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toFloat64((number - 10) / 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;

SELECT toFloat32((number - 10) / 10) AS x, round(x, 1), roundBankers(x, 1), floor(x, 1), ceil(x, 1), trunc(x, 1) FROM system.numbers LIMIT 20;
SELECT toFloat64((number - 10) / 10) AS x, round(x, 1), roundBankers(x, 1), floor(x, 1), ceil(x, 1), trunc(x, 1) FROM system.numbers LIMIT 20;

SELECT toUInt8(number) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT toUInt16(number) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT toUInt32(number) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT toUInt64(number) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT toInt8(number - 10) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT toInt16(number - 10) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT toInt32(number - 10) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT toInt64(number - 10) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT toFloat32(number - 10) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT toFloat64(number - 10) AS x, round(x, -1), roundBankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;

SELECT toUInt8(number) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT toUInt16(number) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT toUInt32(number) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT toUInt64(number) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT toInt8(number - 10) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT toInt16(number - 10) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT toInt32(number - 10) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT toInt64(number - 10) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT toFloat32(number - 10) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT toFloat64(number - 10) AS x, round(x, -2), roundBankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;

SELECT 123456789 AS x, floor(x, -1), floor(x, -2), floor(x, -3), floor(x, -4), floor(x, -5), floor(x, -6), floor(x, -7), floor(x, -8), floor(x, -9), floor(x, -10);
SELECT 12345.6789 AS x, floor(x, -1), floor(x, -2), floor(x, -3), floor(x, -4), floor(x, -5), floor(x, 1), floor(x, 2), floor(x, 3), floor(x, 4), floor(x, 5);


SELECT roundToExp2(100), roundToExp2(64), roundToExp2(3), roundToExp2(0), roundToExp2(-1);
SELECT roundToExp2(0.9), roundToExp2(0), roundToExp2(-0.5), roundToExp2(-0.6), roundToExp2(-0.2);

select round(2, 4) round2, round(20, 4) round20, round(200, 4) round200, round(5, 4) round5, round(50, 4) round50, round(500, 4) round500, round(toInt32(5), 4) roundInt5, round(toInt32(50), 4) roundInt50, round(toInt32(500), 4) roundInt500;
select roundBankers(2, 4) round2, roundBankers(20, 4) round20, roundBankers(200, 4) round200, roundBankers(5, 4) round5, roundBankers(50, 4) round50, roundBankers(500, 4) round500, roundBankers(toInt32(5), 4) roundInt5, roundBankers(toInt32(50), 4) roundInt50, roundBankers(toInt32(500), 4) roundInt500;

SELECT ceil(29375422, -54212) --{serverError 69}
