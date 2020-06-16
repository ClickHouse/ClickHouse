SELECT uniq(number >= 10 ? number : NULL) FROM numbers(10);
SELECT uniqExact(number >= 10 ? number : NULL) FROM numbers(10);
SELECT count(DISTINCT number >= 10 ? number : NULL) FROM numbers(10);

SELECT uniq(number >= 5 ? number : NULL) FROM numbers(10);
SELECT uniqExact(number >= 5 ? number : NULL) FROM numbers(10);
SELECT count(DISTINCT number >= 5 ? number : NULL) FROM numbers(10);

SELECT count(NULL);
-- These two returns NULL for now, but we want to change them to return 0.
SELECT uniq(NULL);
SELECT count(DISTINCT NULL);
