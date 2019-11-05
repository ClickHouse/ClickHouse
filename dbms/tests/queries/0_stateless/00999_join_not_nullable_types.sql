SET join_use_nulls = 1;

SELECT * FROM
(
    SELECT number, ['left'] as ar, number AS left_number FROM system.numbers LIMIT 2
)
FULL JOIN
(
    SELECT number, ['right'] as ar, number AS right_number FROM system.numbers LIMIT 1, 2
)
USING (number)
ORDER BY number;

SELECT * FROM
(
    SELECT ['left'] as ar, number AS left_number FROM system.numbers LIMIT 2
)
FULL JOIN
(
    SELECT ['right'] as ar, number AS right_number FROM system.numbers LIMIT 1, 2
)
ON left_number = right_number
ORDER BY left_number;

SELECT * FROM
(
    SELECT ['left'] as ar, 42 AS left_number
)
FULL JOIN
(
    SELECT ['right'] as ar, 42 AS right_number
)
USING(ar)
ORDER BY left_number;
