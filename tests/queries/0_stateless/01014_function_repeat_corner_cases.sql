SELECT length(repeat('x', 1000000));
SELECT length(repeat('', 1000000));
SELECT length(repeat('x', 1000001)); -- { serverError TOO_LARGE_STRING_SIZE }
SET max_memory_usage = 100000000;
SELECT length(repeat(repeat('Hello, world!', 1000000), 10)); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT repeat(toString(number), number) FROM system.numbers LIMIT 11;
