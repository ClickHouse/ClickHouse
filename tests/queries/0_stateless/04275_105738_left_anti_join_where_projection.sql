-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105738
-- Sibling reproducer for https://github.com/ClickHouse/ClickHouse/issues/99959.
-- In a LEFT ANTI JOIN, the result contains only rows from the left table that have no
-- matching row in the right table. The right-table columns (including the join key)
-- must contain default values for those unmatched rows, not the left key value.
-- The #105738 reproducer self-joins a MergeTree table and varies the presence of a
-- WHERE predicate, which previously could pick different plans and expose the
-- right-key projection inconsistency.

DROP TABLE IF EXISTS m_105738;

CREATE TABLE m_105738 (c0 Int32, c1 UInt64, c2 UInt64) ENGINE = MergeTree() ORDER BY (-c0);
INSERT INTO m_105738 VALUES (1, 100, 999);
INSERT INTO m_105738 VALUES (2, 200, 888);
OPTIMIZE TABLE m_105738 FINAL;

SELECT '--- LEFT ANTI JOIN, baseline (no WHERE) ---';
SELECT right_0.c1
FROM m_105738 LEFT ANTI JOIN m_105738 AS right_0 ON m_105738.c2 = right_0.c1
ORDER BY 1;

SELECT '--- LEFT ANTI JOIN, with WHERE on left ---';
SELECT right_0.c1
FROM m_105738 LEFT ANTI JOIN m_105738 AS right_0 ON m_105738.c2 = right_0.c1
WHERE m_105738.c0
ORDER BY 1;

SELECT '--- LEFT ANTI JOIN, parallel_hash ---';
SELECT right_0.c1
FROM m_105738 LEFT ANTI JOIN m_105738 AS right_0 ON m_105738.c2 = right_0.c1
WHERE m_105738.c0
ORDER BY 1
SETTINGS join_algorithm = 'parallel_hash';

SELECT '--- LEFT ANTI JOIN, grace_hash ---';
SELECT right_0.c1
FROM m_105738 LEFT ANTI JOIN m_105738 AS right_0 ON m_105738.c2 = right_0.c1
WHERE m_105738.c0
ORDER BY 1
SETTINGS join_algorithm = 'grace_hash';

DROP TABLE m_105738;
