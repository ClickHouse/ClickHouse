-- { echo }

-- no clash name
SELECT
  c + 1,
  Z.c + 1
FROM
  (SELECT 10 a) X
CROSS JOIN
  (SELECT 20 b) Y
CROSS JOIN
  (SELECT 30 c) Z;

-- alias clash
SELECT
  (a + 1) AS c,
  Z.c + 1
FROM
  (SELECT 10 a) X
CROSS JOIN
  (SELECT 20 b) Y
CROSS JOIN
  (SELECT 30 c) Z;

-- column clash
SELECT
  (X.c + 1) AS c,
  Z.c + 1
FROM
  (SELECT 10 c) X
CROSS JOIN
  (SELECT 20 b) Y
CROSS JOIN
  (SELECT 30 c) Z;

SELECT
   (X.a + 1) AS a,
   (Y.a + 1) AS Y_a,
   (Z.a + 1) AS Z_a,
   (Y.b + 1) AS b,
   (Z.b + 1) AS Z_b
FROM
  (SELECT 10 a) X
CROSS JOIN
  (SELECT 20 a, 21 as b) Y
CROSS JOIN
  (SELECT 30 a, 31 as b, 32 as c) Z;
