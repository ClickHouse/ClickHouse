-- A bucket count larger than the partition puts every row in its own bucket; counts above the largest signed 64-bit value are rejected.
SELECT number, ntile(toUInt8(255)) OVER (ORDER BY number) FROM numbers(3);
SELECT number, ntile(toUInt32(4294967295)) OVER (ORDER BY number) FROM numbers(3);
SELECT number, ntile(toUInt64(4294967296)) OVER (ORDER BY number) FROM numbers(3);
SELECT number, ntile(9223372036854775807) OVER (ORDER BY number) FROM numbers(3);
SELECT number, ntile(9223372036854775808) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT number, ntile(18446744073709551615) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT number, ntile(toUInt64(18446744073709551615)) OVER (ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
