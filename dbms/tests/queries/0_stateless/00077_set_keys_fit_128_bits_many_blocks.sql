SET max_block_size = 1000;

SELECT number FROM 
(
    SELECT * FROM system.numbers LIMIT 10000
) 
WHERE (number, number * 2) IN 
(
    SELECT number, number * 2 
    FROM system.numbers 
    WHERE number % 1000 = 1 
    LIMIT 2
)
LIMIT 2;
