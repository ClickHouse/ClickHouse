SELECT SUM(LENGTH(arr))
FROM (
  SELECT
    arrayRemove(range(number % 10), x)
    FROM (SELECT * FROM system.numbers LIMIT 1000)
    CROSS JOIN lateral arrayEnumerate(range(number % 10)) AS t(idx, x)
    WHERE LENGTH(toString(x)) % 2 = 0
) AS tmp(arr);

SELECT SUM(LENGTH(arr))
FROM (
  SELECT
    arrayRemove(range(number % 10), x)
    FROM (SELECT * FROM system.numbers LIMIT 1000)
    CROSS JOIN lateral arrayEnumerate(range(number % 10)) AS t(idx, x)
    WHERE LENGTH(x) % 2 = 0
) AS tmp(arr);
