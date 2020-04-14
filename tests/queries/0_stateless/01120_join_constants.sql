SELECT
    t1.*,
    t2.*,
    'world',
    isConstant('world')
FROM
(
    SELECT
        arrayJoin([1, 2]) AS k,
        'hello'
) AS t1
LEFT JOIN
(
    SELECT
        arrayJoin([1, 3]) AS k,
        'world'
) AS t2 ON t1.k = t2.k;
