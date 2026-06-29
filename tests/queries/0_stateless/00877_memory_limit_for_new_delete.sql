-- Tags: no-tsan, no-asan, no-msan, no-parallel, no-fasttest
-- Tag no-msan: memory limits don't work correctly under msan because it replaces malloc/free

SET max_memory_usage = 1000000000;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT sum(ignore(*)) FROM (
    SELECT number, argMax(number, (number, toFixedString(toString(number), 1024)))
    FROM numbers(1000000)
    GROUP BY number
) -- { serverError MEMORY_LIMIT_EXCEEDED }
