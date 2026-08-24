-- { echo }
SELECT sumWithOverflow(a - 65537) FROM (SELECT cast(number AS Decimal32(4)) a FROM numbers(10));
