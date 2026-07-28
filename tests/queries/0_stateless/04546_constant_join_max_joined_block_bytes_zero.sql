-- A cartesian product with `max_joined_block_size_bytes = 0` (no output size limit in bytes) and with both
-- output block limits disabled.

SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;

SELECT count(), sum(a.number + b.number) FROM numbers(100) AS a CROSS JOIN numbers(100) AS b
SETTINGS max_joined_block_size_bytes = 0;

SELECT count(), sum(a.number + b.number) FROM numbers(100) AS a CROSS JOIN numbers(100) AS b
SETTINGS max_joined_block_size_bytes = 0, max_joined_block_size_rows = 0;
