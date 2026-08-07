SELECT * FROM (
    SELECT 1 AS a, 2 AS b FROM system.one
    JOIN system.one USING dummy
      UNION ALL
    SELECT 3 AS a, 4 AS b FROM system.one
)
WHERE a != 10
ORDER BY a, b;
