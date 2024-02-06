SELECT count() FROM generate_series(5, 4);
SELECT count() FROM generate_series(0, 0);
SELECT count() FROM generate_series(10, 20, 3);
SELECT count() FROM generate_series(7, 77, 10);
SELECT count() FROM generate_series(0, 1000000000, 2);
SELECT count() FROM generate_series(0, 999999999, 20);
SELECT count() FROM generate_series(0, 1000000000, 2) WHERE generate_series % 5 == 0;

SELECT * FROM generate_series(5, 4);
SELECT * FROM generate_series(0, 0);
SELECT * FROM generate_series(10, 20, 3);
SELECT * FROM generate_series(7, 77, 10);
SELECT * FROM generate_series(7, 52, 5) WHERE generate_series >= 13;

