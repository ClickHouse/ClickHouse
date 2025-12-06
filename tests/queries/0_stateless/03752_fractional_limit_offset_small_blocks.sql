SET max_block_size = 10;

SELECT 'Fractional Limit Only - Early Pushing';

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9;

SELECT 'Fractional Limit, Normal Offset - Early Pushing/Evicting';

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 10;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9 OFFSET 5;

SELECT 'Fractional Limit, Fractional Offset - Early Pushing/Evicting';

SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 0.25;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.5 OFFSET 0.5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number LIMIT 0.9 OFFSET 0.25;

SELECT 'Fractional Offset Only - Early Evicting';

SELECT number FROM numbers(20) ORDER BY number OFFSET 0.5;
SELECT '*';
SELECT number FROM numbers(20) ORDER BY number OFFSET 0.9;

SELECT 'Big Tables';

DROP TABLE IF EXISTS num_tab;
CREATE TABLE num_tab
ENGINE = MergeTree
ORDER BY number
AS SELECT number FROM numbers(1000000);

SELECT 
    number 
FROM 
    num_tab 
ORDER BY number
LIMIT 10 
OFFSET 0.99999;

SET max_block_size = 7;
SELECT '*';

SELECT
    number
FROM 
    num_tab
ORDER BY number
LIMIT 10 
OFFSET 0.99999;
