SELECT bitmapMax(argMax(x, y))
FROM remote('127.0.0.{2,3}', view(
    SELECT
        groupBitmapState(toUInt32(number)) AS x,
        number AS y
    FROM numbers(10)
    GROUP BY number
));
