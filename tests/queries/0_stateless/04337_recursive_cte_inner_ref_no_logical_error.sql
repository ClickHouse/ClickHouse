SET enable_analyzer = 1;

-- Test for https://github.com/ClickHouse/ClickHouse/issues/89844

WITH RECURSIVE
    d (lp) AS (SELECT 1 UNION ALL SELECT lp + 1 FROM d WHERE lp < 5),
    t (n) AS (SELECT 1 UNION ALL SELECT t.n + d.lp FROM t, d WHERE d.lp = 1 AND t.n < 5)
SELECT max(n) FROM t;

WITH RECURSIVE
    d (z, lp) AS (SELECT '1', 1 UNION ALL SELECT CAST(lp + 1 AS TEXT), lp + 1 FROM d WHERE lp < 9),
    grid AS (SELECT lp AS pos FROM d),
    t (s, next_pos) AS (
        SELECT CAST('' AS TEXT), (SELECT min(pos) FROM grid)
        UNION ALL
        SELECT concat(t.s, m.z), t.next_pos + 1 FROM t, d AS m WHERE m.lp = t.next_pos AND t.next_pos < 5
    )
SELECT count() FROM t;
