SELECT singleValueOrNullMerge(s)
FROM
(
    SELECT singleValueOrNullState(number) AS s
    FROM numbers(2)
);
