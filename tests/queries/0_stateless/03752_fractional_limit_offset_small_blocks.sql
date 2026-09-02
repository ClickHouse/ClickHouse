
SELECT 'Fractional Limit Only - Early Pushing';

SET max_block_size = 10;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9;
SELECT '*';
SELECT number FROM numbers(10) ORDER BY number LIMIT 0.1;
SELECT '*';

SET max_block_size = 3;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9;
SELECT '*';
SELECT number FROM numbers(10) ORDER BY number LIMIT 0.1;

SELECT 'Fractional Limit, Normal Offset - Early Pushing/Evicting';

SET max_block_size = 10;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25 OFFSET 12;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 10;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(15) ORDER BY number LIMIT 0.5 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(14) ORDER BY number LIMIT 0.5 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 15;
SELECT '*';

SET max_block_size = 3;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25 OFFSET 12;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 10;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(15) ORDER BY number LIMIT 0.5 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(14) ORDER BY number LIMIT 0.5 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.05 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 15;

SELECT 'Fractional Limit, Fractional Offset - Early Pushing/Evicting';

SET max_block_size = 10;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 0.25;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25 OFFSET 0.6;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 0.5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9 OFFSET 0.25;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25 OFFSET 0.25;
SELECT '*';
SELECT number FROM numbers(15) ORDER BY number LIMIT 0.5 OFFSET 0.33;
SELECT '*';

SET max_block_size = 3;

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 0.25;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.25 OFFSET 0.6;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 0.5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9 OFFSET 0.25;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.05 OFFSET 0.25;
SELECT '*';
SELECT number FROM numbers(15) ORDER BY number LIMIT 0.5 OFFSET 0.33;

SELECT 'Fractional Offset Only - Early Evicting';

SET max_block_size = 10;

SELECT number FROM numbers(20) ORDER BY number OFFSET 0.5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number OFFSET 0.9;
SELECT '*';
SELECT number FROM numbers(10) ORDER BY number OFFSET 0.1;
SELECT '*';

SET max_block_size = 3;

SELECT number FROM numbers(20) ORDER BY number OFFSET 0.5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number OFFSET 0.9;
SELECT '*';
SELECT number FROM numbers(10) ORDER BY number OFFSET 0.1;
