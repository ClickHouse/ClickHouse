SELECT sumIf(1, 0) FROM numbers(100);
SELECT countIf(0) FROM numbers(100);

SELECT sumIf(1, 1) FROM numbers(100);
SELECT countIf(1) FROM numbers(100);

SELECT sum(if(1 > 0, 1, 0)) FROM numbers(100);
SELECT countIf(1 > 0) FROM numbers(100);

-- default = 1
SET optimize_rewrite_sum_if_to_count_if = 0;

SELECT sumIf(1, 0) FROM numbers(100);
SELECT countIf(0) FROM numbers(100);

SELECT sumIf(1, 1) FROM numbers(100);
SELECT countIf(1) FROM numbers(100);

SELECT sum(if(1 > 0, 1, 0)) FROM numbers(100);
SELECT countIf(1 > 0) FROM numbers(100);
