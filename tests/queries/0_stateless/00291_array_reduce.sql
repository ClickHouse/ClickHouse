SELECT
    arrayReduce('uniq', [1, 2, 1]) AS a,
    arrayReduce('uniq', [1, 2, 2, 1], ['hello', 'world', '', '']) AS b,
    arrayReduce('uniqUpTo(5)', [1, 2, 2, 1], materialize(['hello', 'world', '', ''])) AS c,
    arrayReduce('uniqExactIf', [1, 2, 3, 4], [1, 0, 1, 1]) AS d;

SELECT arrayReduce('quantiles(0.5, 0.9)', range(number) AS r), r FROM system.numbers LIMIT 12;
