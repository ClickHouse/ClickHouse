SELECT count() FROM generate_series(5, 4);
SELECT count() FROM generate_series(0, 0);
SELECT count() FROM generate_series(10, 20, 3);
SELECT count() FROM generate_series(7, 77, 10);
SELECT count() FROM generate_series(0, 1000, 2);
SELECT count() FROM generate_series(0, 999, 20);
SELECT sum(generate_series) FROM generate_series(4, 1008, 4) WHERE generate_series % 7 = 1;
SELECT sum(generate_series) FROM generate_series(4, 1008, 4) WHERE generate_series % 7 = 1 SETTINGS max_block_size = 71;

SELECT * FROM generate_series(5, 4);
SELECT * FROM generate_series(0, 0);
SELECT * FROM generate_series(10, 20, 3);
SELECT * FROM generate_series(7, 77, 10);
SELECT * FROM generate_series(7, 52, 5) WHERE generate_series >= 13;

