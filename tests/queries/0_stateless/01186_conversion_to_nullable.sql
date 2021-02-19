select toUInt8(x) from values('x Nullable(String)', '42', NULL, '0', '', '256');
select toInt64(x) from values('x Nullable(String)', '42', NULL, '0', '', '256');

select toDate(x) from values('x Nullable(String)', '2020-12-24', NULL, '0000-00-00', '', '9999-01-01');
select toDateTime(x) from values('x Nullable(String)', '2020-12-24 01:02:03', NULL, '0000-00-00 00:00:00', '');
select toDateTime64(x, 2) from values('x Nullable(String)', '2020-12-24 01:02:03', NULL, '0000-00-00 00:00:00', '');
select toUnixTimestamp(x) from values ('x Nullable(String)', '2000-01-01 13:12:12', NULL, '');

select toDecimal32(x, 2) from values ('x Nullable(String)', '42', NULL, '3.14159');
select toDecimal64(x, 8) from values ('x Nullable(String)', '42', NULL, '3.14159');

select toString(x) from values ('x Nullable(String)', '42', NULL, 'test');
select toFixedString(x, 8) from values ('x Nullable(String)', '42', NULL, 'test');
