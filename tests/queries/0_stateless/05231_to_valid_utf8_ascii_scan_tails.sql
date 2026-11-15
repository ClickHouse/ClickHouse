-- Valid ASCII, including partial vectors and embedded NULs, must preserve every byte.
SELECT countIf(toValidUTF8(s) != s)
FROM (SELECT repeat('a', number) AS s FROM numbers(145));

SELECT countIf(toValidUTF8(s) != s)
FROM (SELECT repeat('\0a', number) AS s FROM numbers(73));

-- Alternate complete Unicode sequences with ASCII runs of varying lengths.
SELECT countIf(toValidUTF8(s) != s)
FROM
(
    SELECT concat(
        repeat('é', number % 23), repeat('x', number % 33),
        repeat('€', number % 19), repeat('y', number % 17), '🙂') AS s
    FROM numbers(1024)
);

-- A valid ASCII or Unicode tail must separate invalid bytes from subsequent values.
SELECT countIf(toValidUTF8(concat(prefix, '\xFF', tail)) != concat(prefix, '\xEF\xBF\xBD', tail))
FROM
(
    SELECT repeat('a', number % 33) AS prefix,
        concat(repeat('é', number % 11), repeat('\0', number % 17)) AS tail
    FROM numbers(561)
);
