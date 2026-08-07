SELECT arrayDistinct(arrayMap(x -> 0, range(2))) FROM numbers(2);

SELECT arrayDistinct(materialize([[0], [0]])) FROM numbers(2);
SELECT arrayDistinct(materialize(['', '', ''])) FROM numbers(2);
SELECT arrayDistinct(materialize([0, 0, 0])) FROM numbers(2);
SELECT arrayDistinct(materialize([0, 1, 1, 0])) FROM numbers(2);
SELECT arrayDistinct(materialize(['', 'Hello', ''])) FROM numbers(2);


SELECT arrayDistinct(materialize([[0], [0]])) FROM numbers(2);
SELECT arrayDistinct(materialize(['', NULL, ''])) FROM numbers(2);
SELECT arrayDistinct(materialize([0, NULL, 0])) FROM numbers(2);
SELECT arrayDistinct(materialize([0, 1, NULL, 0])) FROM numbers(2);
SELECT arrayDistinct(materialize(['', 'Hello', NULL])) FROM numbers(2);
