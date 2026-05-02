-- Tests for `GROUP BY key WITH CLUSTER d` over String / FixedString keys.
-- Distance is byte-level Levenshtein. d is interpreted as integer max edits.
-- UTF-8 bytes are treated as raw bytes (no codepoint awareness).

SELECT '--- Basic: two close strings, d=1 ---';
SELECT s, count() AS c
FROM VALUES('s String', ('apple'), ('apple'), ('apples'), ('banana'))
GROUP BY s WITH CLUSTER 1
ORDER BY s;

SELECT '--- Chain: a -> ab -> abc, d=1 ---';
-- a and abc differ by 2 edits, but the chain a-ab-abc is transitive.
SELECT count() AS num_groups, sum(c) AS total
FROM (
    SELECT s, count() AS c
    FROM VALUES('s String', ('a'), ('ab'), ('abc'))
    GROUP BY s WITH CLUSTER 1
);

SELECT '--- No merge: gap > d ---';
SELECT s, count()
FROM VALUES('s String', ('apple'), ('orange'))
GROUP BY s WITH CLUSTER 2
ORDER BY s;

SELECT '--- d=0 behaves like plain GROUP BY ---';
SELECT s, count()
FROM VALUES('s String', ('foo'), ('foo'), ('bar'))
GROUP BY s WITH CLUSTER 0
ORDER BY s;

SELECT '--- Single character substitutions, d=1 ---';
-- cat, bat, hat all pairwise within 1 edit of each other.
SELECT count() AS num_groups
FROM (
    SELECT s
    FROM VALUES('s String', ('cat'), ('bat'), ('hat'), ('dog'))
    GROUP BY s WITH CLUSTER 1
);

SELECT '--- Empty string + non-empty ---';
SELECT count() AS num_groups
FROM (
    SELECT s
    FROM VALUES('s String', (''), ('a'), ('b'))
    GROUP BY s WITH CLUSTER 1
);

SELECT '--- Single row ---';
SELECT s, count()
FROM VALUES('s String', ('only'))
GROUP BY s WITH CLUSTER 5;

SELECT '--- With non-cluster key: clusters do not cross categories ---';
SELECT cat, any(s), count() AS c
FROM VALUES('cat String, s String',
    ('a', 'foo'), ('a', 'foo2'),
    ('b', 'foo'), ('b', 'bar'))
GROUP BY cat, s WITH CLUSTER 1
ORDER BY cat, c DESC;

SELECT '--- Multiple aggregates ---';
SELECT count() AS groups, sum(c) AS total, sum(s_total) AS sum_v
FROM (
    SELECT s, count() AS c, sum(v) AS s_total
    FROM VALUES('s String, v UInt32',
        ('apple', 1), ('apple', 2), ('apples', 3),
        ('banana', 10))
    GROUP BY s WITH CLUSTER 1
);

SELECT '--- Large d collapses everything in same non-cluster group ---';
SELECT count() AS num_groups
FROM (
    SELECT s
    FROM VALUES('s String', ('a'), ('zz'), ('hello'), ('world'))
    GROUP BY s WITH CLUSTER 100
);

SELECT '--- FixedString key ---';
SELECT s, count()
FROM VALUES('s FixedString(3)', ('abc'), ('abd'), ('xyz'))
GROUP BY s WITH CLUSTER 1
ORDER BY s;

SELECT '--- Insertions and deletions, d=2 ---';
-- "abcd" vs "ad" differ by 2 (delete b, c).
SELECT count() AS num_groups
FROM (
    SELECT s FROM VALUES('s String', ('abcd'), ('ad'), ('xyz')) GROUP BY s WITH CLUSTER 2
);

SELECT '--- Transposition costs 2 edits (insert + delete), not 1 ---';
-- "ab" vs "ba": Levenshtein distance = 2 (no transposition op).
-- d=1 → no merge; d=2 → merge.
SELECT count() AS groups_d1
FROM (SELECT s FROM VALUES('s String', ('ab'), ('ba')) GROUP BY s WITH CLUSTER 1);
SELECT count() AS groups_d2
FROM (SELECT s FROM VALUES('s String', ('ab'), ('ba')) GROUP BY s WITH CLUSTER 2);

SELECT '--- Length filter: 1 vs 5-char string, d=2 → no merge ---';
SELECT count() AS num_groups
FROM (SELECT s FROM VALUES('s String', ('a'), ('hello')) GROUP BY s WITH CLUSTER 2);

SELECT '--- EXPLAIN PIPELINE shows ClusterMergingTransform ---';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT s, count() FROM VALUES('s String', ('a'), ('b')) GROUP BY s WITH CLUSTER 1
)
WHERE explain LIKE '%ClusterMerging%';
