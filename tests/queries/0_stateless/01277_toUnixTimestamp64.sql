-- Error cases
SELECT toUnixTimestamp64Second();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT toUnixTimestamp64Milli();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT toUnixTimestamp64Micro();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT toUnixTimestamp64Nano();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT toUnixTimestamp64Second('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toUnixTimestamp64Milli('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toUnixTimestamp64Micro('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toUnixTimestamp64Nano('abc');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT toUnixTimestamp64Second('abc', 123);  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT toUnixTimestamp64Milli('abc', 123);  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT toUnixTimestamp64Micro('abc', 123);  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT toUnixTimestamp64Nano('abc', 123);  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT 'const column';
WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS dt64
SELECT dt64, toUnixTimestamp64Second(dt64), toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS dt64
SELECT dt64, toUnixTimestamp64Second(dt64), toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS dt64
SELECT dt64, toUnixTimestamp64Second(dt64), toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

SELECT 'non-const column';
WITH toDateTime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS x
SELECT materialize(x) as dt64, toUnixTimestamp64Second(dt64), toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS x
SELECT materialize(x) as dt64, toUnixTimestamp64Second(dt64), toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);

WITH toDateTime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS x
SELECT materialize(x) as dt64, toUnixTimestamp64Second(dt64), toUnixTimestamp64Milli(dt64), toUnixTimestamp64Micro(dt64), toUnixTimestamp64Nano(dt64);
