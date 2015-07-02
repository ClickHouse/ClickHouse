SELECT
    number,
    range(number) AS arr,
    replicate(number, arr),
    replicate(toString(number), arr),
    replicate(range(number), arr),
    replicate(arrayMap(x -> toString(x), range(number)), arr)
FROM system.numbers
LIMIT 10;
