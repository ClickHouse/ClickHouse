SELECT uniq(number >= 10 ? number : NULL) FROM numbers(10);
SELECT uniqExact(number >= 10 ? number : NULL) FROM numbers(10);
SELECT count(DISTINCT number >= 10 ? number : NULL) FROM numbers(10);

SELECT uniq(number >= 5 ? number : NULL) FROM numbers(10);
SELECT uniqExact(number >= 5 ? number : NULL) FROM numbers(10);
SELECT count(DISTINCT number >= 5 ? number : NULL) FROM numbers(10);

SELECT '---';
SELECT count(NULL);
SELECT uniq(NULL);
SELECT count(DISTINCT NULL);

SELECT '---';
SELECT avg(NULL);
SELECT sum(NULL);
SELECT corr(NULL, NULL);
SELECT corr(1, NULL);
SELECT corr(NULL, 1);
