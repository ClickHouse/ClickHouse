SET max_threads = 1; -- keep the `additional_memory_tracking_per_thread` reservation deterministic
SET max_memory_usage = 20194304; -- 16 MB + 4 MiB to absorb `additional_memory_tracking_per_thread`

SET max_joined_block_size_rows = 10000000;
SELECT count(*) FROM numbers(10000) n1 CROSS JOIN numbers(1000) n2; -- { serverError MEMORY_LIMIT_EXCEEDED }

SET max_joined_block_size_rows = 1000;
SELECT count(*) FROM numbers(10000) n1 CROSS JOIN numbers(1000) n2;
