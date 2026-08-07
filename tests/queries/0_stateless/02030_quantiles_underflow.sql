SELECT
    arrayMap(y -> round(y, 1), quantilesExactInclusive(0.1, 0.9)(x)) AS q
FROM
(
    SELECT arrayJoin([-2147483648, 1, 2]) AS x
);
