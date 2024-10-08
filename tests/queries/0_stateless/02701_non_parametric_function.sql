SELECT * FROM system.numbers WHERE number > toUInt64(10)(number) LIMIT 10; -- { serverError 309 }
