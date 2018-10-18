SELECT
    uniq(x), uniq((x)), uniq(x, y), uniq((x, y)), uniq(x, y, z), uniq((x, y, z)),
    uniqCombined(x), uniqCombined((x)), uniqCombined(x, y), uniqCombined((x, y)), uniqCombined(x, y, z), uniqCombined((x, y, z)),
    uniqCombined(17)(x), uniqCombined(17)((x)), uniqCombined(17)(x, y), uniqCombined(17)((x, y)), uniqCombined(17)(x, y, z), uniqCombined(17)((x, y, z)),
    uniqHLL12(x), uniqHLL12((x)), uniqHLL12(x, y), uniqHLL12((x, y)), uniqHLL12(x, y, z), uniqHLL12((x, y, z)),
    uniqExact(x), uniqExact((x)), uniqExact(x, y), uniqExact((x, y)), uniqExact(x, y, z), uniqExact((x, y, z)),
    uniqUpTo(5)(x), uniqUpTo(5)((x)), uniqUpTo(5)(x, y), uniqUpTo(5)((x, y)), uniqUpTo(5)(x, y, z), uniqUpTo(5)((x, y, z))
FROM
(
    SELECT
        number % 10 AS x,
        intDiv(number, 10) % 10 AS y,
        toString(intDiv(number, 100) % 10) AS z
    FROM system.numbers LIMIT 1000
);


SELECT k,
    uniq(x), uniq((x)), uniq(x, y), uniq((x, y)), uniq(x, y, z), uniq((x, y, z)),
    uniqCombined(x), uniqCombined((x)), uniqCombined(x, y), uniqCombined((x, y)), uniqCombined(x, y, z), uniqCombined((x, y, z)),
    uniqCombined(17)(x), uniqCombined(17)((x)), uniqCombined(17)(x, y), uniqCombined(17)((x, y)), uniqCombined(17)(x, y, z), uniqCombined(17)((x, y, z)),
    uniqHLL12(x), uniqHLL12((x)), uniqHLL12(x, y), uniqHLL12((x, y)), uniqHLL12(x, y, z), uniqHLL12((x, y, z)),
    uniqExact(x), uniqExact((x)), uniqExact(x, y), uniqExact((x, y)), uniqExact(x, y, z), uniqExact((x, y, z)),
    uniqUpTo(5)(x), uniqUpTo(5)((x)), uniqUpTo(5)(x, y), uniqUpTo(5)((x, y)), uniqUpTo(5)(x, y, z), uniqUpTo(5)((x, y, z)),
    count() AS c
FROM
(
    SELECT
        (number + 0x8ffcbd8257219a26) * 0x66bb3430c06d2353 % 131 AS k,
        number % 10 AS x,
        intDiv(number, 10) % 10 AS y,
        toString(intDiv(number, 100) % 10) AS z
    FROM system.numbers LIMIT 100000
)
GROUP BY k
ORDER BY c DESC, k ASC
LIMIT 10;
