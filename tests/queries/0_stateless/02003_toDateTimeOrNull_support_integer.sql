-- { echo }
SELECT toDate(123) = toDateOrNull(123);
SELECT toDate(123) = toDateOrNull(toInt8(123));
SELECT toDate(123) = toDateOrZero(toInt16(123));
SELECT toDate(123) = toDateOrZero(toInt32(123));
SELECT toDate(123) = toDateOrNull(toInt64(123));

SELECT toDate(123) = toDateOrZero(toUInt8(123));
SELECT toDate(123) = toDateOrNull(toUInt16(123));
SELECT toDate(123) = toDateOrNull(toUInt32(123));
SELECT toDate(123) = toDateOrZero(toUInt64(123));

SELECT toDateOrNull('');
SELECT toDate32OrNull('');

SELECT toDateTimeOrNull('');
SELECT toDateTimeOrZero('', 'Asia/Shanghai');

-- SELECT toDateTimeOrZero(1583851242, 'Asia/Shanghai');
-- SELECT toDateTimeOrNull(1583851242, 'Asia/Shanghai');
-- SELECT toDateTimeOrNull(1583851242, 'Asia/Shanghai') = toDateTime(1583851242, 'Asia/Shanghai');
-- SELECT toDateTime(1583851242, 'Asia/Shanghai') = toDateTimeOrZero(1583851242, 'Asia/Shanghai');
-- SELECT toDateTime(-1, 'Asia/Shanghai');
-- SELECT toDateTimeOrNull(-1, 'Asia/Shanghai');
-- SELECT toDateTimeOrZero(-1, 'Asia/Shanghai');

SELECT toDateOrNull(-1), toDate32OrNull(-1);
SELECT toDateOrZero(-1), toDate32OrZero(-1);
SELECT toDate(1), toDate32(1);
SELECT toDateOrNull(toInt64(2147483647)), toDate32OrNull(toInt64(2147483647));
SELECT toDateOrZero(toInt32(2147483646)), toDate32OrZero(toInt32(2147483646));
SELECT toDateOrNull(toInt64(2147483648)), toDate32OrNull(toInt64(2147483648));
SELECT toDateOrZero(toInt64(2147483648)), toDate32OrZero(toInt64(2147483648));

