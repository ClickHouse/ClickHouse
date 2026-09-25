-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/69078
SELECT toInt32OrDefault(CAST('42424242424.424242', 'Decimal64(6)'));
SELECT toInt32OrDefault(CAST('42424242424.424242', 'Decimal64(6)'), CAST(-7, 'Int32'));
SELECT accurateCastOrNull(CAST('42424242424.424242', 'Decimal64(6)'), 'Int32');

SELECT toInt32OrDefault(CAST('2147483647.999999', 'Decimal64(6)'), CAST(-7, 'Int32'));
SELECT toInt32OrDefault(CAST('-2147483648.999999', 'Decimal64(6)'), CAST(-7, 'Int32'));
SELECT toInt32OrDefault(CAST('2147483648.000000', 'Decimal64(6)'), CAST(-7, 'Int32'));
SELECT toInt32OrDefault(CAST('-2147483649.000000', 'Decimal64(6)'), CAST(-7, 'Int32'));

SELECT toUInt64OrDefault(CAST('-1', 'Decimal64(0)'), CAST(9, 'UInt64'));
SELECT accurateCastOrNull(CAST('-1', 'Decimal64(0)'), 'UInt64');

SELECT toInt32OrDefault(value, CAST(-7, 'Int32'))
FROM
(
    SELECT arrayJoin([
        CAST('42.424242', 'Decimal64(6)'),
        CAST('42424242424.424242', 'Decimal64(6)')
    ]) AS value
);