SET enable_analyzer = 1;

SELECT count(*)
FROM
(
    SELECT
        ['a', 'b'] AS a1,
        [1] AS a2
) AS bb
ARRAY JOIN
    a2,
    a1
SETTINGS enable_unaligned_array_join = 1
;

SELECT count(*)
FROM
(
    SELECT
        ['a', 'b'] AS a1,
        [1] AS a2
) AS bb
ARRAY JOIN
    a1,
    a2
SETTINGS enable_unaligned_array_join = 1
;
