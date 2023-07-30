SELECT
    number / 8 AS width,
    bar(width, 0, 3, 3) AS b,
    bar(width - 0.001, 0, 3, 3) AS `b_minus`,
    hex(b),
    hex(b_minus)
FROM numbers(20);
