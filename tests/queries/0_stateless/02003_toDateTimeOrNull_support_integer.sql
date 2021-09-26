SELECT toDate(123) = toDateOrNull(123);
SELECT toDate(123) = toDateOrNull(toInt8(123));
SELECT toDate(123) = toDateOrZero(toInt16(123));
SELECT toDate(123) = toDateOrZero(toInt32(123));
SELECT toDate(123) = toDateOrNull(toInt64(123));

SELECT toDateOrNull(toInt8(123));
SELECT toDateOrZero(toInt16(123));
SELECT toDateOrZero(toInt32(1230));
SELECT toDateOrNull(toInt64(12300));

SELECT toDate(123) = toDateOrZero(toUInt8(123));
SELECT toDate(123) = toDateOrNull(toUInt16(123));
SELECT toDate(123) = toDateOrNull(toUInt32(123));
SELECT toDate(123) = toDateOrZero(toUInt64(123));

SELECT toDateOrZero(toUInt8(123));
SELECT toDateOrNull(toUInt16(123));
SELECT toDateOrNull(toUInt32(1230));
SELECT toDateOrZero(toUInt64(12300));

SELECT toDateOrNull('');
SELECT toDate32OrNull('');

SELECT toDateTimeOrNull('');
SELECT toDateTimeOrZero('', 'Asia/Shanghai');

SELECT toDateTimeOrZero(1583851242, 'Asia/Shanghai');
SELECT toDateTimeOrNull(1583851242, 'Asia/Shanghai');

SELECT toDateTimeOrNull(1583851242, 'Asia/Shanghai') = toDateTime(1583851242, 'Asia/Shanghai');
SELECT toDateTime(1583851242, 'Asia/Shanghai') = toDateTimeOrZero(1583851242, 'Asia/Shanghai');
