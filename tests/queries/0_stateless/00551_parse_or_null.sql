SELECT toUInt64OrZero(s), toUInt64OrNull(s) FROM (SELECT CASE WHEN number % 2 = 1 THEN toString(number) ELSE 'hello' END AS s FROM system.numbers) LIMIT 10;
SELECT toUInt64OrZero(s), toUInt64OrNull(s) FROM (SELECT CASE WHEN number = 5 THEN NULL WHEN number % 2 = 1 THEN toString(number) ELSE 'hello' END AS s FROM system.numbers) LIMIT 10;
