-- An aggregate function state can serialize to zero bytes: a Resample range with begin >= end holds
-- no nested states, and a matrix aggregate function called with no arguments holds no cells. Such a
-- state is a valid (empty) hash table key.

SELECT length(countResampleMergeDistinct(10, 5, 1)(s))
FROM (SELECT countResampleState(10, 5, 1)(number, number) AS s FROM numbers(10) GROUP BY number);

SELECT length(groupUniqArray(s))
FROM (SELECT countResampleState(10, 5, 1)(number, number) AS s FROM numbers(10) GROUP BY number);

SELECT length(groupArrayIntersect(a))
FROM (SELECT [countResampleState(10, 5, 1)(number, number)] AS a FROM numbers(10) GROUP BY number);

-- Controls: a non-empty range must keep working.
SELECT countResampleMergeDistinct(1, 5, 1)(s)
FROM (SELECT countResampleState(1, 5, 1)(number, number) AS s FROM numbers(10) GROUP BY number);

SELECT length(groupUniqArray(s))
FROM (SELECT countResampleState(1, 5, 1)(number, number) AS s FROM numbers(10) GROUP BY number);

SELECT length(groupArrayIntersect(a))
FROM (SELECT [countResampleState(1, 5, 1)(number, number)] AS a FROM numbers(10) GROUP BY number);

-- A zero-byte state that does not come from the Resample combinator.
SELECT length(groupUniqArray(s)) FROM (SELECT covarSampMatrixState() AS s FROM numbers(3));
