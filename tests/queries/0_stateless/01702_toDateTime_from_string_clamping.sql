SELECT toString(toDateTime('-922337203.6854775808', 1, 'Asia/Istanbul'));
SELECT toString(toDateTime('9922337203.6854775808', 1, 'Asia/Istanbul'));
SELECT toDateTime64(CAST('10500000000.1' AS Decimal64(1)), 1, 'Asia/Istanbul');
SELECT toDateTime64(CAST('-10500000000.1' AS Decimal64(1)), 1, 'Asia/Istanbul');
