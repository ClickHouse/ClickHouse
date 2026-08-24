SELECT min(number), max(number), sum(number) FROM numbers_mt(10000000);
SELECT min(number), max(number), sum(number) FROM numbers(10000000, 5000000);
SELECT min(number), max(number), sum(number) FROM numbers_mt(10000000, 5000000);
