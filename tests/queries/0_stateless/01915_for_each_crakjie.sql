WITH arrayJoin(['a', 'b']) AS z
SELECT
    z,
    sumMergeForEach(x) AS x
FROM
(
    SELECT sumStateForEach([1., 1.1, 1.1300175]) AS x
    FROM remote('127.0.0.{1,2}', system.one)
)
GROUP BY z
ORDER BY z;
