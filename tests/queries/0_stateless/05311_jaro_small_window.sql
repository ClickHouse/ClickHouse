-- Only c can match. Exercise both edges of the Jaro window, bit 63, unequal
-- lengths, and the 64/65-byte dispatch boundary with NUL and high-bit bytes.
WITH
    p.1 AS n,
    p.2 AS m,
    p.3 AS i,
    p.4 AS j,
    greatest(0, intDiv(greatest(toInt32(n), toInt32(m)), 2) - 1) AS distance,
    concat(repeat('a', i), c, repeat('a', n - i - 1)) AS s1,
    concat(repeat('b', j), c, repeat('b', m - j - 1)) AS s2,
    if(abs(toInt32(i) - toInt32(j)) <= distance, (1.0 / n + 1.0 / m + 1.0) / 3.0, 0.0) AS expected,
    if(expected > 0.7 AND i = 0 AND j = 0, expected + 0.1 * (1.0 - expected), expected) AS expected_winkler
SELECT
    countIf(NOT (abs(jaroSimilarity(materialize(s1), materialize(s2)) - expected) <= 1e-14)),
    countIf(NOT (abs(jaroWinklerSimilarity(materialize(s1), materialize(s2)) - expected_winkler) <= 1e-14))
FROM
(
    SELECT arrayJoin([
        (1, 1, 0, 0), (1, 2, 0, 0), (2, 2, 0, 1),
        (16, 16, 0, 7), (16, 16, 0, 8), (17, 17, 0, 7), (17, 17, 0, 8),
        (32, 32, 0, 15), (32, 32, 0, 16), (33, 33, 0, 15), (33, 33, 0, 16),
        (64, 64, 0, 31), (64, 64, 0, 32), (64, 64, 32, 63), (64, 64, 31, 63),
        (64, 64, 63, 32), (64, 64, 63, 31), (64, 1, 31, 0), (64, 1, 32, 0),
        (1, 64, 0, 31), (1, 64, 0, 32), (65, 65, 64, 33), (65, 65, 64, 32),
        (65, 64, 64, 33), (65, 64, 64, 32)
    ]) AS p
)
CROSS JOIN (SELECT arrayJoin(['\0', '\xff']) AS c);
