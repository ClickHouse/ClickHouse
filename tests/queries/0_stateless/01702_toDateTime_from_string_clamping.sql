-- { echo }
SELECT toString(toDateTime('-922337203.6854775808', 1));
SELECT toString(toDateTime('9922337203.6854775808', 1));
SELECT toDateTime64(CAST('10000000000.1' AS Decimal64(1)), 1);
SELECT toDateTime64(CAST('-10000000000.1' AS Decimal64(1)), 1);
