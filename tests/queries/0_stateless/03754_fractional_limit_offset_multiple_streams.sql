
SELECT 'Fractional Limit Only';

SET max_block_size = 65409;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.9);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.1);
SELECT '*';

SET max_block_size = 10;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.9);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.1);
SELECT '*';

SET max_block_size = 3;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.9);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.1);

SELECT 'Fractional Limit, Normal Offset';

SET max_block_size = 65409;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 500);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 1200);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 1000);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.9 OFFSET 500);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 500);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 1500);
SELECT '*';

SET max_block_size = 10;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 500);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 1200);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 1000);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.9 OFFSET 500);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 500);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 1500);
SELECT '*';

SET max_block_size = 3;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 500);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 1200);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 1000);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.9 OFFSET 500);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 500);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 1500);

SELECT 'Fractional Limit, Fractional Offset';

SET max_block_size = 65409;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 0.25);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 0.6);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 0.5);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.9 OFFSET 0.25);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 0.25);
SELECT '*';

SET max_block_size = 10;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 0.25);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 0.6);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 0.5);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.9 OFFSET 0.25);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 0.25);
SELECT '*';

SET max_block_size = 3;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 0.25);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 0.6);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.5 OFFSET 0.5);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.9 OFFSET 0.25);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 0.25 OFFSET 0.25);

SELECT 'Fractional Offset Only';

SET max_block_size = 65409;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) OFFSET 0.5);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) OFFSET 0.9);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) OFFSET 0.1);
SELECT '*';

SET max_block_size = 10;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) OFFSET 0.5);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) OFFSET 0.9);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) OFFSET 0.1);
SELECT '*';

SET max_block_size = 3;

SELECT count() FROM (SELECT number FROM numbers_mt(2000) OFFSET 0.5);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) OFFSET 0.9);
SELECT '*';
SELECT count() FROM (SELECT number FROM numbers_mt(2000) OFFSET 0.1);
