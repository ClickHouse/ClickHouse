select icebergHash(toDecimal64(1.23, 8)) from numbers(10);
select icebergHash(toUUID('0e3bcf76-323d-4e5f-a4f6-65e4b948a4e1')) from numbers(10);
select icebergHash(toDateTime64('2025-11-18 17:48:12.123456789', 9, 'UTC')) from numbers(10);
