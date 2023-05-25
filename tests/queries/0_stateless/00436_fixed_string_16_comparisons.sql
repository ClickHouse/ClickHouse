SELECT
    a, b, a = b, a != b, a < b, a > b, a <= b, a >= b,
    toFixedString(a, 16) AS fa, toFixedString(b, 16) AS fb, fa = fb, fa != fb, fa < fb, fa > fb, fa <= fb, fa >= fb
FROM
(
    SELECT 'aaaaaaaaaaaaaaaa' AS a
    UNION ALL SELECT 'aaaaaaaaaaaaaaab'
    UNION ALL SELECT 'aaaaaaaaaaaaaaac'
    UNION ALL SELECT 'baaaaaaaaaaaaaaa'
    UNION ALL SELECT 'baaaaaaaaaaaaaab'
    UNION ALL SELECT 'baaaaaaaaaaaaaac'
    UNION ALL SELECT 'aaaaaaaabaaaaaaa'
    UNION ALL SELECT 'aaaaaaabaaaaaaaa'
    UNION ALL SELECT 'aaaaaaacaaaaaaaa'
) js1
CROSS JOIN
(
    SELECT 'aaaaaaaaaaaaaaaa' AS b
    UNION ALL SELECT 'aaaaaaaaaaaaaaab'
    UNION ALL SELECT 'aaaaaaaaaaaaaaac'
    UNION ALL SELECT 'baaaaaaaaaaaaaaa'
    UNION ALL SELECT 'baaaaaaaaaaaaaab'
    UNION ALL SELECT 'baaaaaaaaaaaaaac'
    UNION ALL SELECT 'aaaaaaaabaaaaaaa'
    UNION ALL SELECT 'aaaaaaabaaaaaaaa'
    UNION ALL SELECT 'aaaaaaacaaaaaaaa'
) js2
ORDER BY a, b;


SELECT
    toFixedString(a, 16) AS a,
    toFixedString('aaaaaaaaaaaaaaaa', 16) AS b1,
    toFixedString('aaaaaaaaaaaaaaab', 16) AS b2,
    toFixedString('aaaaaaaaaaaaaaac', 16) AS b3,
    toFixedString('baaaaaaaaaaaaaaa', 16) AS b4,
    toFixedString('baaaaaaaaaaaaaab', 16) AS b5,
    toFixedString('baaaaaaaaaaaaaac', 16) AS b6,
    toFixedString('aaaaaaaabaaaaaaa', 16) AS b7,
    toFixedString('aaaaaaabaaaaaaaa', 16) AS b8,
    toFixedString('aaaaaaacaaaaaaaa', 16) AS b9,
    a = b1, a != b1, a < b1, a > b1, a <= b1, a >= b1,
    a = b2, a != b2, a < b2, a > b2, a <= b2, a >= b2,
    a = b3, a != b3, a < b3, a > b3, a <= b3, a >= b3,
    a = b4, a != b4, a < b4, a > b4, a <= b4, a >= b4,
    a = b5, a != b5, a < b5, a > b5, a <= b5, a >= b5,
    a = b6, a != b6, a < b6, a > b6, a <= b6, a >= b6,
    a = b7, a != b7, a < b7, a > b7, a <= b7, a >= b7,
    a = b8, a != b8, a < b8, a > b8, a <= b8, a >= b8,
    a = b9, a != b9, a < b9, a > b9, a <= b9, a >= b9
FROM
(
    SELECT 'aaaaaaaaaaaaaaaa' AS a
    UNION ALL SELECT 'aaaaaaaaaaaaaaab'
    UNION ALL SELECT 'aaaaaaaaaaaaaaac'
    UNION ALL SELECT 'baaaaaaaaaaaaaaa'
    UNION ALL SELECT 'baaaaaaaaaaaaaab'
    UNION ALL SELECT 'baaaaaaaaaaaaaac'
    UNION ALL SELECT 'aaaaaaaabaaaaaaa'
    UNION ALL SELECT 'aaaaaaabaaaaaaaa'
    UNION ALL SELECT 'aaaaaaacaaaaaaaa'
)
ORDER BY a;
