-- Bugs and edge cases found by `gtest_functions_stress` and fixed in this PR.

-- `negate` overflow at type minimum: wraps around (no UB)
SELECT negate(toInt64(-9223372036854775808));
SELECT negate(toInt8(-128));

-- `intExp2` is not injective for floats: multiple inputs give same output
SELECT intExp2(1.0), intExp2(1.5), intExp2(1.9);

-- `intExp10` is not injective for floats
SELECT intExp10(1.0), intExp10(1.5), intExp10(1.9);

-- `toStartOfMillisecond` / `toStartOfMicrosecond` with extreme DateTime64 (no signed overflow UB)
SELECT toStartOfMillisecond(toDateTime64('2100-01-01 00:00:00', 2));
SELECT toStartOfMillisecond(toDateTime64('1900-01-01 00:00:00', 1));
SELECT toStartOfMicrosecond(toDateTime64('2100-01-01 00:00:00', 2));

-- Modulo sign consistency: const vs non-const for power-of-two divisors
SELECT -1::Int32 % 4::Int32, materialize(-1::Int32) % 4::Int32;
SELECT -7::Int64 % 8::Int64, materialize(-7::Int64) % 8::Int64;
SELECT -1::Int8 % 2::Int8, materialize(-1::Int8) % 2::Int8;

-- CASE with Int128 types (falls back to `multiIf` from `transform`)
SELECT CASE 0 WHEN 0 THEN 1::Int128 WHEN 1 THEN 2::Int128 ELSE 3::Int128 END;
SELECT CASE 1 WHEN 0 THEN 1::Int128 WHEN 1 THEN 2::Int128 ELSE 3::Int128 END;
SELECT CASE 5 WHEN 0 THEN 1::Int128 WHEN 1 THEN 2::Int128 ELSE 3::Int128 END;

-- `if` condition normalization (non-boolean UInt8 condition)
SELECT if(toUInt8(2), 10, 20);
SELECT if(toUInt8(0), 10, 20);
SELECT if(toUInt8(2), 1.5, 2.5);
SELECT if(toUInt8(0), 1.5, 2.5);

-- `trimRight` with too-long trim character string
SELECT trimRight('hello', repeat('x', 257)); -- { serverError TOO_LARGE_STRING_SIZE }

-- `parseDateTime` with insufficient input
SELECT parseDateTime('42', '%f'); -- { serverError CANNOT_PARSE_DATETIME }

-- `fromModifiedJulianDayOrNull` with out-of-range inputs returns NULL
SELECT fromModifiedJulianDayOrNull(toInt32(-1000000));
SELECT fromModifiedJulianDayOrNull(toInt32(99999999));

-- Interval conversion for non-const columns: each row processed individually
SELECT CAST(number AS IntervalDay) FROM numbers(3);

-- `conv` with invalid base
SELECT conv('abc', 1, 10); -- { serverError BAD_ARGUMENTS }

-- `tupleElement` with out-of-bound negative index
SELECT (1, 2).-4; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- NaN deduplication in `arrayDistinct` (ColumnUnique NaN normalization)
SELECT arrayDistinct([nan::Float32, -nan::Float32])::Array(Float32);
