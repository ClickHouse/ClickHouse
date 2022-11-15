-- Error cases
SELECT toUnixTimestamp64Milli();  -- {serverError 42}
SELECT toUnixTimestamp64Micro();  -- {serverError 42}
SELECT toUnixTimestamp64Nano();  -- {serverError 42}

SELECT toUnixTimestamp64Milli('abc');  -- {serverError 43}
SELECT toUnixTimestamp64Micro('abc');  -- {serverError 43}
SELECT toUnixTimestamp64Nano('abc');  -- {serverError 43}

SELECT toUnixTimestamp64Milli('abc', 123);  -- {serverError 42}
SELECT toUnixTimestamp64Micro('abc', 123);  -- {serverError 42}
SELECT toUnixTimestamp64Nano('abc', 123);  -- {serverError 42}

SELECT 'const column';
WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS dt64
SELECT dt64, toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS dt64
SELECT dt64, toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS dt64
SELECT dt64, toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

SELECT 'non-const column';
WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS x
SELECT materialize(x) as dt64, toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS x
SELECT materialize(x) as dt64, toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS x
SELECT materialize(x) as dt64, toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

