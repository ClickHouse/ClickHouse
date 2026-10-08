
-- This test ensures that using the `WITH TIES` operator will have no 
-- effect on the results that doesn't contain ties and hence will return
-- the same result as 03752_fractional_limit_offset_small_block.sql
-- except for the 'Fractiona Offset Only' block which is not included.

SELECT 'Fractional Limit Only - Early Pushing';

SET max_block_size = 10;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9 WITH TIES;
SELECT '*';
SELECT number FROM numbers(10) ORDER BY number LIMIT 0.1 WITH TIES;
SELECT '*';

SET max_block_size = 3;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9 WITH TIES;
SELECT '*';
SELECT number FROM numbers(10) ORDER BY number LIMIT 0.1 WITH TIES;

SELECT 'Fractional Limit, Normal Offset - Early Pushing/Evicting';

SET max_block_size = 10;

SELECT number FROM numbers(20) ORDER BY number LIMIT 5, 0.5   WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 12, 0.25 WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 10, 0.5  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 5, 0.9   WITH TIES;
SELECT '*';
SELECT number FROM numbers(15) ORDER BY number LIMIT 5, 0.5   WITH TIES;
SELECT '*';
SELECT number FROM numbers(14) ORDER BY number LIMIT 5, 0.5   WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 5, 0.25  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 15, 0.5  WITH TIES;
SELECT '*';

SET max_block_size = 3;

SELECT number FROM numbers(20) ORDER BY number LIMIT 5, 0.5   WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 12, 0.25 WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 10, 0.5  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 5, 0.9   WITH TIES;
SELECT '*';
SELECT number FROM numbers(15) ORDER BY number LIMIT 5, 0.5   WITH TIES;
SELECT '*';
SELECT number FROM numbers(14) ORDER BY number LIMIT 5, 0.5   WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 5, 0.05  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 15, 0.5  WITH TIES;

SELECT 'Fractional Limit, Fractional Offset - Early Pushing/Evicting';

SET max_block_size = 10;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25, 0.5  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.6, 0.25  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5, 0.5   WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25, 0.9  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25, 0.25 WITH TIES;
SELECT '*';
SELECT number FROM numbers(15) ORDER BY number LIMIT 0.33, 0.5  WITH TIES;
SELECT '*';

SET max_block_size = 3;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25, 0.5  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.6, 0.25  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5, 0.5   WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25, 0.9  WITH TIES;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25, 0.05 WITH TIES;
SELECT '*';
SELECT number FROM numbers(15) ORDER BY number LIMIT 0.33, 0.5  WITH TIES;
